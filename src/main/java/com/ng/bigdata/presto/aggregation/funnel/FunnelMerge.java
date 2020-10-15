package com.ng.bigdata.presto.aggregation.funnel;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.function.*;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.iq80.leveldb.table.BlockBuilder;

/**
 * @User: kaisy
 * @Date: 2020/10/9 18:40
 * @Desc: 计算漏斗结果
 */
@AggregationFunction("funnel_merge")
public class FunnelMerge {
    @InputFunction
    public static void input(SliceState state,
                             @SqlType(StandardTypes.INTEGER) long userState,
                             @SqlType(StandardTypes.INTEGER) long events_count){
        // todo 初始化state
        Slice slice = state.getSlice();
        // todo 初始化state，长度[events num]
        if (slice == null) {
            slice = Slices.allocate((int) events_count * 4);
        }
        // todo 计算每个时间下的用户数量，按照用户的深度计算设置相应事件位置的值
        // tip 比如，用户漏斗深度为3
        for (int status = 0; status < userState; status++) {
            int index = status * 4;
            slice.setInt(index, slice.getInt(index) + 1);
        }
        // todo 返回状态
        state.setSlice(slice);

    }

    @CombineFunction
    public static void combine(SliceState state1, SliceState state2) {
        // todo 获取状态
        Slice slice1 = state1.getSlice();
        Slice slice2 = state2.getSlice();

        // todo 更新状态
        if (slice1 == null) {
            state1.setSlice(slice2);
        } else {
            for (int index = 0; index < slice1.length(); index++) {
                slice1.setInt(index, slice1.getInt(index) + slice2.getInt(index));
            }
            // tip 返回状态
            state1.setSlice(slice1);
        }
    }

    @OutputFunction("array<bigint>")
    public static void output(SliceState state, BlockBuilder out) {
        // todo 获取状态
        Slice slice = state.getSlice();
        if (slice == null) {

        }
    }

}


















