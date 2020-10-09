package com.ng.bigdata.presto.aggregation;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.spi.function.*;

/**
 * @User: kaisy
 * @Date: 2020/10/9 18:42
 * @Desc: 聚合函数，Double小数的相加
 */

@AggregationFunction("sum_double")
@Description("this is double sum function")
public class DoubleSumAggregation {
    private DoubleSumAggregation() {
    }

    public static void input(@AggregationState NullableDoubleState state,
                             @SqlType(StandardTypes.DOUBLE) double d) {
        state.setNull(false);
        state.setDouble(state.getDouble() + d);
    }

    /**
     * 中间聚合
     * @param state1
     * @param state2
     */
    @CombineFunction
    public static void combine(@AggregationState NullableDoubleState state1,
                               @AggregationState NullableDoubleState state2) {
        if (state1.isNull()) {
            state1.setNull(false);
            state1.setDouble(state2.getDouble());
            return;
        }
        state1.setDouble(state1.getDouble() + state2.getDouble());
    }

    /**
     * 进行结果输出
     * @param state
     * @param out
     */
    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState NullableDoubleState state,
                              BlockBuilder out){
        NullableDoubleState.write(DoubleType.DOUBLE,state,out);

    }
}












