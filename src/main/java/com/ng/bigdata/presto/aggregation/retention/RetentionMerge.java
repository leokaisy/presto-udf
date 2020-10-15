package com.ng.bigdata.presto.aggregation.retention;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.*;
import com.qf.bigdata.presto.aggregation.SliceState;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.common.type.TypeUtils.readNativeValue;

/**
 * @Description: 计算 日，周，月 留存，第二阶段函数
 * 第一阶段函数的用户转态作为输入。
 * Input例子，比如查的是发生A事件的1天，之后3天每天发生B事件的留存用户，第一阶段输出结果如下:
 *      user1 [1,7]  表示user1发了A事件，之后在第一，二，三 天都发生了B事件  7=4+2+1
 *      user2 [1,6]  表示user1发了A事件，之后只在第二，三天发生了B事件      6=4+2
 *      user3 [1,5]  表示user1发了A事件，之后只在第一，三天发生了B事件      5=1+4
 *
 * 之后根据输入，合并计算每一天留存的总人数，举个例子，说明此函数的输出
 * eg:
 * 查询2020620到2020621日发生AppClick事件的用户，在之后3天，每天又再次发生该行为的用户
 * select retention_merge(user_state, 2, 3) from(
 * select distinct_id, retention(
 * date_diff('day', from_iso8601_timestamp('2007-01-01'), from_unixtime(ctime/1000)),
 * date_diff('day', from_iso8601_timestamp('2007-01-01'), from_iso8601_timestamp('2020-06-20')),
 * 2, 3, event,'AppClick,AppPageView', 'AppClick,AppPageView') as user_state
 * from ods_news.event
 * where (logday >= '20200620' and logday < '20200622' and event in ('AppClick')) or
 *     (logday >= '20200621' and logday < '20200625' and event in ( 'AppClick'))
 * group by distinct_id
 * )
 * 此函数返回如下类似结果：
 * [5777, 5357, 2205, 5395, 2211, 0, 7608, 7618]
 *
 *
 * 我们要查的是起始事件2天，每天之后3天的留存
 * 数组中的前三个值 【5777, 5357, 2205】 表示 20200620这一天的用户，在接下来三天每天的留存，比如：5777表示  20200620这一天的用户在20200621这一天又发生AppClick事件的用户，依次类推
 * 数组中的前4~6个值 【5395, 2211, 0】 表示 20200621这一天的用户，在接下来三天每天的留存， 比如：5395  20200621这一天的用户在20200622这一天又发生AppClick事件的用户，依次类推
 *
 * 数组中第7个值，表示20200620这一天发生起始事件AppClick的总用户数据 【注意因为我们的查询起始事件和结束事件相同，这里表示的起始事件】
 * 数组中第8个值，表示20200621这一天发生起始事件AppClick的总用户数据 【注意因为我们的查询起始事件和结束事件相同，这里表示的起始事件】
 *
 * 这里大家要根据你要做的查询去理解查询结果，不同起始事件范围和留存范围，会有不同的数组大小。
 *
 *
 *
 *
 * @Author: QF
 * @Date: 2020/6/22 2:09 PM
 * @Version V1.0
 */
@AggregationFunction("retention_merge")
public class RetentionMerge extends Base {

    /**
     *
     * @param state 存储数据, 根据需要初始化其大小
     * @param userState  每个用户状态，每个输入应该是 [起始时间状态值,结束事件状态值]或者叫[first事件状态值,sencond事件状态值]
     *                   例如 [3,5] 表示用户在第 1，2 两个状态位有值， 5表示用户在第 1,3 两个状态位有值
     *                   1      2       4   ...  8   i << 1
     *                   -----------------------------------------
     *                   1      1       0       3= 1+2
     *                   1      0       1       5= 1+4
     *
     * @param first_length 表示first事件的长度，具体含义就是指要计算的起始事件的日期跨度，定义一个日期06-01，此时如果这个值是 2,
     *                     就表示计算 06-01~06-02 这两天每天的留存，至于是多少天的留存看下面second_length 这个参数
     *                     其值最大是63
     * @param second_length 表示second事件的长度，具体含义就是指要计算的结束事件的日期跨度，定义一个日期06-01，此时如果这个值是 3,
     *                      就表示计算 3 日留存，那对于06-01来讲，就是计算 06-02 06-03 06-04 这三天留存。 对于06-02，就是计算
     *                       06-03 06-04 06-05 这三天留存
     */
    @InputFunction
    public static void input(SliceState state,
                             @SqlType("array<bigint>") Block userState,  // 每个用户的状态
                             @SqlType(StandardTypes.INTEGER) long first_length,         // 当前查询的first长度(15, 12, 6)
                             @SqlType(StandardTypes.INTEGER) long second_length) {      // 当前查询的second长度(30, 8, 3)
        // 获取状态
        Slice slice = state.getSlice();

        // 初始化state
        if (null == slice) {

            /*
             *  比如 first_length =2  second_length=3
             *  我们最终需要的结果是 [x,x,x,  x,x,x,  x,x] 总共8个位置, 最后两个位置代表first_length=2 需要两个位置存储当天的数据
             *  前面两个 x,x,x  代表在first_length的每一天，留存second_length=3的每天的数据位置 , 正好 2 * 3 =6 个位置
             *
             *  first_length，这个slice的大小就确定了 比如上边的 first_length =2  second_length=3
             *  slice大小就是 （2*3 +2）* 4   乘以4因为int类型，占4个字节
             *
             */
            slice = Slices.allocate((int) (first_length * second_length + first_length) * 4);
        }

        // 获取UserState值,比如我们之前举例 [3,5]
        // first_value = 3
        // second_value = 5
        long first_value = (long) readNativeValue(BigintType.BIGINT, userState, 0);
        long second_value = (long) readNativeValue(BigintType.BIGINT, userState, 1);

        // 计算用户在每个位置的状态值
        for (int i = 0; i < first_length; ++i) {
            // 判断是否更改first计数， 这个就是拿我们取出来的first_value值，它代表了用户在起始事件每天的状态
            // 比如 3 , 然后和我们预先定义一个对应的位置索引值 [1,2,4,8.....] 取 按位与操作
            // 比如第一次循环 3 & 1 不等于0, 代表在第一个位置有值，也就是用户在这一天出现过，第二次循环 3&2 不等于0，代表在第二个位置有值
            // 也就是在这一天也出现过
            if ((first_value & bit_array_long.get(i)) != 0) {
                // 第一个事件存在, 更改first计数
                int first_index = (int) (first_length * second_length + i) * 4;
                slice.setInt(first_index, slice.getInt(first_index) + 1);

                // 判断是否更改second计数
                // 这个第二层循环，第一层循环我们得到了一个用户在起始事件某天的状态，第二层循环就是判读该用户该天的状态后，
                // second_length 每天后的状态， 比如 second_length=3 ,就是用户在之后三天的每天的状态
                // 这就相当于比如用户 06-01日活跃了，之后三天06-02，06-03，06-64 每天的状态，此时如果second_value=5 表示了用户
                // 在06-02 06-04 这两天活跃
                for (int j = i; j < i + second_length; ++j) {
                    if ((second_value & bit_array_long.get(j)) != 0) {
                        // 第二个事件存在, 更改second计数
                        int second_index = (int) (i * second_length + (j - i)) * 4;
                        slice.setInt(second_index, slice.getInt(second_index) + 1);
                    }
                }
            }
        }

        // 返回状态
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
            for (int index = 0; index < slice.length(); index += 4) {
                slice.setInt(index, slice.getInt(index) + otherslice.getInt(index));
            }
            state.setSlice(slice);
        }
    }

    @OutputFunction("array<bigint>")
    public static void output(SliceState state, BlockBuilder out) {
        // 获取状态
        Slice slice = state.getSlice();
        if (null == slice) {
            BlockBuilder blockBuilder = out.beginBlockEntry();
//            out.writeObject(blockBuilder.build());
            out.closeEntry();
            return;
        }

        // 构造结果: first_length日/周/月中每日/周/月的second_length留存数, 最后为first_length日/周/月的总用户数
        BlockBuilder blockBuilder = out.beginBlockEntry();
        for (int index = 0; index < slice.length(); index += 4) {
            BigintType.BIGINT.writeLong(blockBuilder, slice.getInt(index));
        }

        out.closeEntry();
    }
}
