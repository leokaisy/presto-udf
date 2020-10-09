package com.ng.bigdata.presto.aggregation.funnel;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.function.*;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * @User: kaisy
 * @Date: 2020/10/9 14:30
 * @Desc: 计算漏斗的第一阶段，计算每个用户的有序漏斗深度
 * 计算漏斗的聚合函数, 步骤一
 * <p>
 * 这里以两个事件漏斗举例，可以是多个, 这里的ctime表示事件发生的时间，event表示发生的事件
 * <p>
 * eg: 计算20200923日到20200930日7天内,时间窗口为7天，事件为AppPageView,AppClick两个事件的漏斗
 * <p>
 * select distinct_id ,funnel(ctime, 7*86400000, event, 'AppPageView,AppClick') as user_state
 * from ods_news.event
 * where  event in ('AppPageView','AppClick',) and logday>='20200923' and logday<'20200930'
 * group by distinct_id
 */
@AggregationFunction("funnel")
public class Funnel extends Base {
    // todo 即将将数据存入Slice空间中，但是前提我们要设计存入位数
    // todo 设计初衷 窗口大小[4Byte]，事件个数[4Byte]，事件时间[4Byte]，时间索引[1Byte]
    // 定义两个常量
    // 状态的最左边的两位放临时变量，每个临时变量都为int类型
    private static final int COUNT_FLAG_LENGTH = 8;
    // tip 时间所占位数，事件包含一个Int（时间戳）和一个Byte（事件下标）
    private static final int COUNT_ONE_LENGTH = 5;

    @InputFunction
    public static void input(SliceState state,
                             @SqlType(StandardTypes.BIGINT) long eventTime,  // 时间发生时间
                             @SqlType(StandardTypes.BIGINT) long windows,    // 窗口长度
                             @SqlType(StandardTypes.VARCHAR) Slice event,    // 时间名称
                             @SqlType(StandardTypes.VARCHAR) Slice events) {    // 漏斗全部事件，逗号分隔
        // todo 获取状态
        Slice slice = state.getSlice();
        // todo 初始化状态
        if (!event_pos_dict.containsKey(events)) {
            init_events(events);
        }

        // todo 进行计算
        if (null == slice) {
            // tip 首先分配空间  窗口大小[4Byte]，事件个数[4Byte]，事件时间[4Byte]，时间索引[1Byte]
            slice = Slices.allocate(COUNT_FLAG_LENGTH + COUNT_ONE_LENGTH);
            // tip 将窗口大小 事件个数 添加进入空间
            slice.setInt(0, (int) windows);
            slice.setInt(4, event_pos_dict.get(events).size());
            // tip 时间发生时间 时间索引 添加进空间
            slice.setInt(COUNT_FLAG_LENGTH, (int) eventTime);
            slice.setByte(COUNT_FLAG_LENGTH + 4, event_pos_dict.get(events).get(event));
            // tip 分配完成，将空间数据返回
            state.setSlice(slice);
        } else {
            // tip 首先获取slice长度
            int slice_length = slice.length();
            // tip 新建slice，并在上一个长度后面追加数据
            Slice newSlice = Slices.allocate(slice_length + COUNT_ONE_LENGTH);
            // 将数据存进新的空间
            newSlice.setBytes(0, slice.getBytes());
            newSlice.setInt(slice_length, (int) eventTime);
            newSlice.setByte(slice_length + 4, event_pos_dict.get(events).get(event));
            // tip 将结果返回状态  [事件时间[4Byte],事件索引[1Byte],事件时间[4Byte],事件索引[1Byte]....]
            state.setSlice(newSlice);
        }
    }

    /**
     * 获取中间聚合的状态  窗口大小[4Byte]，事件个数[4Byte]，事件时间[4Byte]，事件索引[1Byte]...
     * 这两个状态数据样式是一样的，不一样的是  一个是新的状态，一个是历史小态
     *
     * @param state1
     * @param state2
     */
    @CombineFunction
    public static void combine(SliceState state1, SliceState state2) {
        // todo 获取状态数据
        Slice slice1 = state1.getSlice();
        Slice slice2 = state2.getSlice();

        // todo 如果为空，将当前状态返回，表示第一次运行
        if (slice1 == null) {
            state1.setSlice(slice2);
        } else {
            // todo 如果不为空，那么首先处理两个数据的长度
            int length1 = slice1.length();
            int length2 = slice2.length();
            // todo 然后新建一个slice将数据合并
            Slice newSlice = Slices.allocate(length1 + length2 - COUNT_FLAG_LENGTH);
            // tip 先将第一个空间数据加入到新空间中
            newSlice.setBytes(0, slice1.getBytes());
            // tip 再将新的空间数据追加至新空间后
            newSlice.setBytes(length1, slice2.getBytes(), COUNT_FLAG_LENGTH, length2 - COUNT_FLAG_LENGTH);
            // tip 最后将新的数据返回
            state1.setSlice(newSlice);
        }
    }

    /**
     * 计算该条数据的深度，将最后结果输出
     *
     * @param state
     * @param out
     */
    @OutputFunction(StandardTypes.INTEGER)
    public static void output(SliceState state, BlockBuilder out) {
        // todo 获取状态
        Slice slice = state.getSlice();
        // todo 判断数据是否为空，若为空返回0
        if (slice == null) {
            out.writeInt(0);
            out.closeEntry();
            return;
        }
        // todo 若数据不为空，开始计算用户深度
        // tip 添加一个临时变量
        boolean is_a = false;
        // tip 构造列表和字典，这个主要是为了后面进行时间排序
        List<Integer> timeArr = new ArrayList<>();
        // tip 创建Map来保存 时间 和 事件索引 （0,1,2,3）
        HashMap<Integer, Byte> timeEventMap = new HashMap<>();
        // 获取事件数据Index，这里使用循环方式循环所有事件数据
        // 循环从第八个字节开始，因为前面8个字节是已经存好了，那么后面就是事件数据
        // 每次循环固定加5，因为格式是8+5
        for (int index = COUNT_FLAG_LENGTH; index < slice.length(); index += COUNT_ONE_LENGTH) {
            // tip 获取事件的时间戳和对应的时间
            int timestamp = slice.getInt(index);
            byte event_index = slice.getByte(index + 4);
            // tip 更新临时变量 如果当event_index等于0，表示第一个事件，如果没有0就证明没有事件，直接返回0
            // 如果等于0，则证明是一个完整的事件，需要重头开始
            if (!is_a && event_index == 0) {
                is_a = true;
            }
            // 将数据追加到timeArr 和 timeEventMap
            timeArr.add(timestamp);
            timeEventMap.put(timestamp, event_index);
        }
        // todo 判断时事件是否符合要求，若不符合要求直接返回0
        if (!is_a) {
            out.writeInt(0);
            out.closeEntry();
            return;
        }
        // todo 若符合要求，按照时间戳数组排序（这个步骤很消耗性能） 正序排序 从小到大排序事件时间
        Collections.sort(timeArr);
        // todo 获取变量
        int windows = slice.getInt(0);
        int event_count = slice.getInt(4);
        // todo 遍历时间戳数据 =》 遍历有序事件，最后构造结果即可
        // tip 定义一个事件深度
        int event_depth = 0;
        List<int[]> temp = new ArrayList<>();
        // tip 循环事件时间，并循环所有事件
        for (int timestamp : timeArr) {
            // 时间时间有序
            Byte event_index = timeEventMap.get(timestamp);
            if (event_index == 0) {
                // 新建临时对象，存放（A事件的时间戳，当前最后一个时间的下标）
                // 创建临时数组保存事件，然后再次加入到集合中，保存开始事件
                int[] flag = {timestamp, event_index};
                temp.add(flag);
            } else {
                // 更新临时对象，从后往前遍历
                for (int i = temp.size() - 1 ; i >= 0; --i) {
                    int[] flag = temp.get(i);
                    // 如果你的当前时间戳大于了窗口大小，表示不合法数据需要跳出循环
                    if (timestamp - flag[0] > windows) {
                        break;
                    } else if (event_index == flag[1] + 1) {
                        // 证明则个数据在窗口内，属合法数据，更新
                        flag[1] = event_index;
                        if (event_depth < event_index) {
                            event_depth = event_index;
                        }
                        break;
                    }
                }
                // 漏斗循环结束，退出即可
                if (event_depth + 1 == event_count) {
                    break;
                }
            }
        }
        // todo 返回结果
        out.writeInt(event_depth + 1);
        out.closeEntry();
    }
}












