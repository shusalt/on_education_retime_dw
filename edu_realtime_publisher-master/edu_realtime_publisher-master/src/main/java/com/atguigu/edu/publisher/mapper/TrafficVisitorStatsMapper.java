package com.atguigu.edu.publisher.mapper;

import com.atguigu.edu.publisher.bean.TrafficVisitorStatsPerHour;
import com.atguigu.edu.publisher.bean.TrafficVisitorTypeStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TrafficVisitorStatsMapper {

    // 分时流量数据查询
    @Select("select\n" +
            "toHour(stt) hr,\n" +
            "sum(uv_count)  uv_count,\n" +
            "sum(page_view_count)  page_view_count,\n" +
            "sum(if(is_new = '1', dws_traffic_vc_source_ar_is_new_page_view_window.uv_count, 0)) new_uv_ct\n" +
            "from dws_traffic_vc_source_ar_is_new_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by hr")
    List<TrafficVisitorStatsPerHour> selectVisitorStatsPerHr(Integer date);

    // 各类访客流量数据查询
    @Select("select\n" +
            "is_new,\n" +
            "sum(uv_count) uv_count,\n" +
            "sum(page_view_count) page_view_count,\n" +
            "sum(total_session_count) total_session_count,\n" +
            "sum(jump_session_count) jump_session_count,\n" +
            "sum(total_during_time) total_during_time\n" +
            "from dws_traffic_vc_source_ar_is_new_page_view_window\n" +
            "where toYYYYMMDD(stt) =#{date}\n" +
            "group by is_new")
    List<TrafficVisitorTypeStats> selectVisitorTypeStats(@Param("date")Integer date);

}
