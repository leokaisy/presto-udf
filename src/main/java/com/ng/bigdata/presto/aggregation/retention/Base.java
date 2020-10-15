package com.ng.bigdata.presto.aggregation.retention;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.*;

/**
 * @Description: 留存的基础信息
 * @Author: QF
 * @Date: 2020/6/22 2:09 PM
 * @Version V1.0
 */
public class Base {

    // 表示第起始时间所能支持的最大数范围，静态数值, 一个short的长度, 这里15就代表着能支持15个时间的范围，
    public static final int MAX_COUNT_SHORT = 15;

    // 表示能够支持的最大留存时间范围， 静态数值, 一个long的长度，这里63就代表着能够支持63个时间范围的流程。
    public static final int MAX_COUNT_LONG = 63;


    // 每一个值代表着每一个位置的用户转态
    // 用于判断某一位为0或1的变量(short类型): [1, 2, 4, ..., 64, ...]
    public static List<Short> bit_array_short = new ArrayList<>();

    // 每一个值代表着每一个位置的用户转态
    // 用于判断某一位为0或1的变量(long类型): [1, 2, 4, ..., 64, ...]
    public static List<Long> bit_array_long = new ArrayList<>();


    // 查询的最大值(short类型), 用于提前退出 [1, 3, 7, 15, ..., 127, ...]
    public static List<Short> max_value_array_short = new ArrayList<>();

    // 查询的最大值(long类型), 用于提前退出 [1, 3, 7, 15, ..., 127, ...]
    public static List<Long> max_value_array_long = new ArrayList<>();

    // 初始化变量
    static {

        for (int i = 0; i < MAX_COUNT_SHORT; ++i) {
            bit_array_short.add((short) (1 << i));
        }

        for (int i = 0; i < MAX_COUNT_LONG; ++i) {
            bit_array_long.add((1L << i));
        }

        short max_value_short = 0;
        for (int i = 0; i < MAX_COUNT_SHORT; ++i) {
            max_value_short += (short) (1 << i);
            max_value_array_short.add(max_value_short);
        }

        long max_value_long = 0;
        for (int i = 0; i < MAX_COUNT_LONG; ++i) {
            max_value_long += (1L << i);
            max_value_array_long.add(max_value_long);
        }
    }

    // 起始事件和下标的对应关系: {events: {event: index, ...}, ....}, 对应flag为1
    public static Map<Slice, Map<Slice, Byte>> event_pos_dict_start = new HashMap<>();

    // 结束事件和下标的对应关系: {events: {event: index, ...}, ....}, 对应flag为2
    public static Map<Slice, Map<Slice, Byte>> event_pos_dict_end = new HashMap<>();

    // 初始化事件和下标的对应关系, flag为0、1或2, flag含义参见上边
    public static void init_events(Slice events, int flag) {
        List<String> fs = Arrays.asList(new String(events.getBytes()).split(","));

        Map<Slice, Byte> pos_dict = new HashMap<>();
        for (byte i = 0; i < fs.size(); ++i) {
            pos_dict.put(Slices.utf8Slice(fs.get(i)), i);
        }

         if (flag == 1) {
            event_pos_dict_start.put(events, pos_dict);
        } else if (flag == 2) {
            event_pos_dict_end.put(events, pos_dict);
        }
    }

}
