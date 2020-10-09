package com.ng.bigdata.presto.scalar;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

/**
 * @User: kaisy
 * @Date: 2020/10/9 18:56
 * @Desc:
 */
public class ScalarFunctions {
    @ScalarFunction("my_upper")  // 函数名
    @Description("大小写转换")  // 注释
    @SqlType(StandardTypes.VARCHAR)
    public static Slice toUpper(@SqlType(StandardTypes.VARCHAR) Slice input){
        return Slices.utf8Slice(input.toStringUtf8().toUpperCase());
    }
}
