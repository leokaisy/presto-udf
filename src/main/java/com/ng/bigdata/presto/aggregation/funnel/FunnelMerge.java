package com.ng.bigdata.presto.aggregation.funnel;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.*;
import com.ng.bigdata.presto.aggregation.SliceState;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

/**
 * @User: kaisy
 * @Date: 2020/10/9 18:40
 * @Desc: 计算漏斗的第二阶段，Merge函数. 根据funnel函数的结果，计算最终漏斗结果
 * 这里以两个事件漏斗举例，可以是多个 这里的ctime表示事件发生的视觉，event表示发生的事件
 * eg:
 * select funnel_merge(user_state,2) from
 * (select distinct_id ,funnel(ctime, 7*86400000, event, 'AppPageView,AppClick') as user_state
 * from ods_news.event
 * where  event in ('AppPageView','AppClick',) and logday>='20200601' and logday<'20200621'
 * group by distinct_id
 * )
 * result样例: [3000,2500], 代表这个两步漏斗第一个事件[AppPageView]人数是3000人，这3000人漏到第二个事件[AppClick]人数是 2500人
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
        // tip eg. 比如用户漏斗深度为3，那么第1，2，3位置都要+1，表示用户发生了3前个事件
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
        // todo 数据为空, 返回一个空数组
        // todo 返回结果 []
        if (null == slice) {
            BlockBuilder blockBuilder = out.beginBlockEntry();
            out.closeEntry();
            return;
        }
        // todo 结果含义: [EVENT-A:3000, EVENT-A:2500, EVENT-A:1000, ......]
        // todo 最终输出结果 [3000,2500,1000]
        BlockBuilder blockBuilder = out.beginBlockEntry();
        for (int index = 0; index < slice.length(); index += 4) {
            BigintType.BIGINT.writeLong(blockBuilder, slice.getInt(index));
        }
        out.closeEntry();
    }

}


















