package com.ng.bigdata.presto.aggregation;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorState;
import io.airlift.slice.Slice;

/**
 * @Description: 定义SliceState 存储状态数据
 * @Author: QF
 * @Date: 2020/6/22 2:09 PM
 * @Version V1.0
 */
public interface SliceState
        extends AccumulatorState {
    Slice getSlice();

    void setSlice(Slice value);

    static void write(Type type, SliceState state, BlockBuilder out) {
        if (state.getSlice() == null) {
            out.appendNull();
        } else {
            type.writeSlice(out, state.getSlice());
        }
    }
}