package com.ng.bigdata.presto;


import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;
import com.ng.bigdata.presto.aggregation.DoubleSumAggregation;
import com.ng.bigdata.presto.aggregation.funnel.Funnel;
import com.ng.bigdata.presto.aggregation.funnel.FunnelMerge;
import com.ng.bigdata.presto.scalar.ScalarFunctions;

import java.util.Set;

/**
 * @User: kaisy
 * @Date: 2020/10/9 19:00
 * @Desc:
 */
public class PrestoUdfPlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions() {
        return ImmutableSet.<Class<?>>builder()
                .add(ScalarFunctions.class)
//                .add(DoubleSumAggregation.class)
                .add(Funnel.class)
                .add(FunnelMerge.class)
                .build();
    }
}
