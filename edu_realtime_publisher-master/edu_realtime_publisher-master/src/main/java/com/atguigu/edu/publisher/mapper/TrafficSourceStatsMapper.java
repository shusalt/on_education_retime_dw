package com.atguigu.edu.publisher.mapper;

import com.atguigu.edu.publisher.bean.*;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;


public interface TrafficSourceStatsMapper {

    // 1. 获取各来源独立访客数
    @Select("select source_name,\n" +
            "       sum(uv_count)           uv_count\n" +
            "from  dws_traffic_vc_source_ar_is_new_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), source_id, source_name\n" +
            "order by uv_count desc;")
    List<TrafficUvCt> selectUvCt(@Param("date") Integer date);

    // 2. 获取各来源会话数
    @Select("select source_name,\n" +
            "       sum(total_session_count)           total_session_count\n" +
            "from  dws_traffic_vc_source_ar_is_new_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), source_id, source_name\n" +
            "order by total_session_count desc;")
    List<TrafficSvCt> selectSvCt(@Param("date") Integer date);

    // 3. 获取各来源会话平均页面浏览数
    @Select("select source_name,\n" +
            "       sum(page_view_count) / sum(total_session_count)   pv_per_session\n" +
            "from  dws_traffic_vc_source_ar_is_new_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), source_id, source_name\n" +
            "order by pv_per_session desc;")
    List<TrafficPvPerSession> selectPvPerSession(@Param("date") Integer date);

    // 4. 获取各来源会话平均页面访问时长
    @Select("select source_name,\n" +
            "       sum(total_during_time) / sum(total_session_count) dur_per_session\n" +
            "from  dws_traffic_vc_source_ar_is_new_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), source_id, source_name\n" +
            "order by dur_per_session desc;")
    List<TrafficDurPerSession> selectDurPerSession(@Param("date") Integer date);

    // 5. 获取各来源跳出率
    @Select("select source_name,\n" +
            "       sum(jump_session_count) / sum(total_session_count)   uj_rate\n" +
            "from  dws_traffic_vc_source_ar_is_new_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), source_id, source_name\n" +
            "order by uj_rate desc;")
    List<TrafficUjRate> selectUjRate(@Param("date") Integer date);

}
