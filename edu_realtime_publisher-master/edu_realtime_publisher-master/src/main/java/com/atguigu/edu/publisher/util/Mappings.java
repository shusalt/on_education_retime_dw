package com.atguigu.edu.publisher.util;

import java.util.HashMap;
import java.util.Map;

/**
 * description:
 * Created by 铁盾 on 2023/3/27
 */
public class Mappings {
    public static Map<String, String> mappings;

    static {
        mappings = new HashMap<>();
        mappings.put("北京", "北京市");
    }

    public static Map<String, String> getMappings() {
        return mappings;
    }
}
