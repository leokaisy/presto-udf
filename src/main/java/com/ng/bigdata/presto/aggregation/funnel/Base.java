package com.ng.bigdata.presto.aggregation.funnel;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @User: kaisy
 * @Date: 2020/10/9 14:19
 * @Desc: 封装漏斗的基础信息
 */
public class Base {
    // todo 漏斗时间和索引关系Map{events：{event:index,....},{...}}
    public static Map<Slice, Map<Slice, Byte>> event_pos_dict = new HashMap<>();

    // todo 让每一个事件携带一个索引
    public static void init_events(Slice events) {
        List<String> fs = Arrays.asList(new String(events.getBytes()).split(","));
        Map<Slice, Byte> pos_dict = new HashMap<>();
        // todo 将所有事件追加至集合
        for (Byte i = 0; i < fs.size(); i++) {
            pos_dict.put(Slices.utf8Slice(fs.get(i)), i);
        }

        event_pos_dict.put(events, pos_dict);
    }


}














