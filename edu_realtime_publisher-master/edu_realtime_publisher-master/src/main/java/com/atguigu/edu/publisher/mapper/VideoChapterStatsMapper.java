package com.atguigu.edu.publisher.mapper;

import com.atguigu.edu.publisher.bean.VedioChapterStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface VideoChapterStatsMapper {

    @Select("select chapter_name,\n" +
            "       sum(play_count)  play_ct,\n" +
            "       sum(play_total_sec)  play_total_sc,\n" +
            "       sum(play_uu_count)  play_uu_ct,\n" +
            "       play_total_sc / play_uu_ct per_uu_sec\n" +
            "from dws_learn_chapter_play_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), chapter_name\n" +
            "order by play_total_sc desc, play_ct desc, play_uu_ct desc\n" +
            "limit 10;")
    List<VedioChapterStats> selectVedioChapterStats(@Param("date")Integer date);
}
