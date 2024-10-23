package com.atguigu.edu.publisher.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * description:
 * Created by 铁盾 on 2022/4/18
 */
public class DateUtil {
    public static Integer now() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
        String date = dtf.format(LocalDateTime.now());
        return Integer.parseInt(date);
    }
}
