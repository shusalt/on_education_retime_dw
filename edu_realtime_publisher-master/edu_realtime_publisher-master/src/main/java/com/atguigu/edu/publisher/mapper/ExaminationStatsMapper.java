package com.atguigu.edu.publisher.mapper;

import com.atguigu.edu.publisher.bean.ExaminationCourseStats;
import com.atguigu.edu.publisher.bean.ExaminationPaperStats;
import com.atguigu.edu.publisher.bean.ExaminationQuestionStats;
import com.atguigu.edu.publisher.bean.ExaminationScoreDurDistribution;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface ExaminationStatsMapper {
    @Select("select paper_title,\n" +
            "       sum(exam_taken_count)  exam_taken_count,\n" +
            "       sum(exam_total_score) / exam_taken_count  exam_avg_score,\n" +
            "       sum(exam_total_during_sec) / exam_taken_count exam_avg_sec\n" +
            "from  dws_examination_paper_exam_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by paper_id, paper_title\n" +
            "order by exam_taken_count desc\n" +
            "limit 10;")
    List<ExaminationPaperStats> selectExaminationPaperStats(@Param("date") Integer date);

@Select("select course_name,\n" +
        "       sum(exam_taken_count)  exam_taken_count,\n" +
        "       sum(exam_total_score) / exam_taken_count  exam_avg_score,\n" +
        "       sum(exam_total_during_sec) / exam_taken_count exam_avg_sec\n" +
        "from  dws_examination_paper_exam_window\n" +
        "where toYYYYMMDD(stt) = #{date}\n" +
        "group by toYYYYMMDD(stt), course_id, course_name\n" +
        "order by exam_taken_count desc\n" +
        "limit 10;")
List<ExaminationCourseStats> selectExaminationCourseStats(@Param("date") Integer date);

@Select("select score_duration,\n" +
        "       sum(user_count) exam_taken_count\n" +
        "from dws_examination_paper_score_duration_exam_window\n" +
        "where toYYYYMMDD(stt) = #{date}\n" +
        "group by score_duration\n" +
        "order by score_duration;")
List<ExaminationScoreDurDistribution> selectExaminationScoreDurDistribution(@Param("date") Integer date);

    @Select("select question_txt,\n" +
            "       sum(correct_answer_count)           correct_answer_count,\n" +
            "       sum(answer_count)                   answer_count,\n" +
            "       round(round(toFloat64(correct_answer_count), 5) " +
            "       / round(toFloat64(answer_count), 5), 20) accuracy_rate\n" +
            "from dws_examination_question_answer_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by question_id,\n" +
            "         question_txt\n" +
            "order by accuracy_rate desc\n" +
            "limit 10;")
    List<ExaminationQuestionStats> selectExaminationQuestionStats(@Param("date") Integer date);
}
