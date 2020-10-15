package com.ng.bigdata.presto.aggregation.retention;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.*;
import com.ng.bigdata.presto.aggregation.SliceState;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;


/**
 * @Description: 计算 日，周，月 留存，第一阶段函数
 * <p>
 * 支持的时间为1.5年。 日就是 450天，周就是 72周， 月就是 18个月
 * 起始事件时间与支持留存时间范围为一下值
 * 15 - 30 表示支持15天的起始事件范围
 * 12 - 8  表示支持12周的起始事件范围，8周留存时间范围
 * 6 -3    表示支持6个月的起始事件范围，3个月的留存时间范围
 * <p>
 * 使用方式如下：
 * 注: 2007-01-01正好为一个周的第一天，且为一个月的第一天，用来校对周月的留存查询
 * select distinct_id, retention(
 * date_diff('day', from_iso8601_timestamp('2007-01-01'), from_unixtime(ctime/1000)),
 * date_diff('day', from_iso8601_timestamp('2007-01-01'), from_iso8601_timestamp('2020-06-20')),
 * 2, 3, event,'AppClick,AppPageView', 'AppClick,AppPageView') as user_state
 * from ods_news.event
 * where (logday >= '20200620' and logday < '20200622' and event in ('AppClick')) or
 * (logday >= '20200621' and logday < '20200625' and event in ( 'AppClick'))
 * group by distinct_id
 * <p>
 * 输出结果类似如下：
 * *      user1 [1,7]  表示user1发了A事件，之后在第一，二，三 天都发生了B事件  7=4+2+1
 * *      user2 [1,6]  表示user1发了A事件，之后只在第二，三天发生了B事件      6=4+2
 * *      user3 [1,5]  表示user1发了A事件，之后只在第一，三天发生了B事件      5=1+4
 * @Author: QF
 * @Date: 2020/6/22 2:09 PM
 * @Version V1.0
 */
@AggregationFunction("retention")
public class Retention extends Base {

    private static final int FIRST = 2;
    private static final int SECOND = 8;
    private static final int INDEX = FIRST;

    @InputFunction
    public static void input(SliceState state,                                      // 存储每个用户的状态
                             @SqlType(StandardTypes.BIGINT) long diffCtime,             // 当前事件的事件距离某固定日期的差值 , 为了计算一个准确的周或者月
                             @SqlType(StandardTypes.BIGINT) long diffStartTime,       // 当前查询的起始日期距离某固定日期的差值,  为了计算一个准确周或者月
                             @SqlType(StandardTypes.INTEGER) long first_length,     // 当前查询的first长度(15天, 12周, 6月)
                             @SqlType(StandardTypes.INTEGER) long second_length,    // 当前查询的second长度(30天, 8周, 3月)
                             @SqlType(StandardTypes.VARCHAR) Slice event,           // 当前事件的名称, A,B,C,D
                             @SqlType(StandardTypes.VARCHAR) Slice events_start,    // 当前查询的起始事件列表, 逗号分隔
                             @SqlType(StandardTypes.VARCHAR) Slice events_end) {    // 当前查询的结束事件列表, 逗号分隔
        // 获取状态
        Slice slice = state.getSlice();
        // 判断是否需要初始化events
        if (!event_pos_dict_start.containsKey(events_start)) {
            init_events(events_start, 1);
        }
        // 判断是否需要初始化events
        if (!event_pos_dict_end.containsKey(events_end)) {
            init_events(events_end, 2);
        }
        // 初始化某一个用户的state, 分别存放不同事件在每个时间段的标示
        if (null == slice) {
            slice = Slices.allocate(FIRST + SECOND);
        }
        // 判读是否为起始事件
        if (event_pos_dict_start.get(events_start).containsKey(event)) {
            int xindex_max = (int) first_length - 1;

            // 获取用户在当前index的状态
            short current_value = slice.getShort(0);
            if (current_value < max_value_array_short.get(xindex_max)) {
                // 获取下标
                int xindex = (int) (diffCtime - diffStartTime);
                if (xindex >= 0 && xindex <= xindex_max) {
                    // 更新状态
                    slice.setShort(0, current_value | bit_array_short.get(xindex));
                }
            }

        }
        // 判断是否为结束事件
        if (event_pos_dict_end.get(events_end).containsKey(event)) {
            int xindex_max = (int) (first_length + second_length - 1) - 1;

            // 获取用户在当前index的状态
            long current_value = slice.getLong(INDEX);
            if (current_value < max_value_array_long.get(xindex_max)) {
                // 获取下标
                int xindex = (int) (diffCtime - (diffStartTime + 1));
                if (xindex >= 0 && xindex <= xindex_max) {
                    // 更新状态
                    slice.setLong(INDEX, current_value | bit_array_long.get(xindex));
                }
            }
        }

        // 返回结果
        state.setSlice(slice);
    }

    @CombineFunction
    public static void combine(SliceState state, SliceState otherState) {
        // 获取状态
        Slice slice = state.getSlice();
        Slice otherslice = otherState.getSlice();

        // 更新状态并返回结果
        if (null == slice) {
            state.setSlice(otherslice);
        } else {
            slice.setShort(0, slice.getShort(0) | otherslice.getShort(0));
            slice.setLong(INDEX, slice.getLong(INDEX) | otherslice.getLong(INDEX));
            state.setSlice(slice);
        }
    }

    @OutputFunction("array<bigint>")
    public static void output(SliceState state, BlockBuilder out) {
        // 获取状态
        Slice slice = state.getSlice();


        // 构造结果: 当前用户在第一个事件中每一天(周/月)的状态, 和在第二个事件中每一天(周/月)的状态
        BlockBuilder blockBuilder = out.beginBlockEntry();

        if (null == slice) {
            BigintType.BIGINT.writeLong(blockBuilder, 0);
            BigintType.BIGINT.writeLong(blockBuilder, 0);
        } else {
            BigintType.BIGINT.writeLong(blockBuilder, slice.getShort(0));
            BigintType.BIGINT.writeLong(blockBuilder, slice.getLong(INDEX));
        }

        // 返回结果
        out.closeEntry();
    }
}
