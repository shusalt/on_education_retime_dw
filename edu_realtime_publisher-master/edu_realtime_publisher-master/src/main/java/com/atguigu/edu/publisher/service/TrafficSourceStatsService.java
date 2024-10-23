package com.atguigu.edu.publisher.service;
import com.atguigu.edu.publisher.bean.*;

import java.util.List;

public interface TrafficSourceStatsService {

    // 1. 获取各来源独立访客数
    List<TrafficUvCt> getUvCt(Integer date);

    // 2. 获取各来源会话数
    List<TrafficSvCt> getSvCt(Integer date);

    // 3. 获取各来源会话平均页面浏览数
    List<TrafficPvPerSession> getPvPerSession(Integer date);

    // 4. 获取各来源会话平均页面访问时长
    List<TrafficDurPerSession> getDurPerSession(Integer date);

    // 5. 获取各来源跳出率
    List<TrafficUjRate> getUjRate(Integer date);

}
