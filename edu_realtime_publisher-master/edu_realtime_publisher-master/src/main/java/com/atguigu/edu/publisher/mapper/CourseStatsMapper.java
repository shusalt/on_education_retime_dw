package com.atguigu.edu.publisher.mapper;

import com.atguigu.edu.publisher.bean.*;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface CourseStatsMapper {

    @Select("select course_name,\n" +
            "       sum(order_total_amount) order_total_amount\n" +
            "from dws_trade_course_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by course_id, course_name\n" +
            "order by order_total_amount desc\n" +
            "limit 10;")
    List<CourseOrderStats> selectCourseOrderStats(@Param("date") Integer date);

    @Select("select subject_name,\n" +
            "       sum(order_total_amount) order_total_amount\n" +
            "from dws_trade_course_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by subject_id, subject_name\n" +
            "order by order_total_amount desc;")
    List<SubjectOrderStats> selectSubjectOrderStats(@Param("date")Integer date);

    @Select("select category_name,\n" +
            "       sum(order_total_amount) order_total_amount\n" +
            "from dws_trade_course_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by category_id, category_name\n" +
            "order by order_total_amount desc")
    List<Category0rderStats> selectCategoryOrderStats(@Param("date") Integer date);

    @Select("select course_name,\n" +
            "       sum(review_total_stars)     review_total_stars,\n" +
            "       sum(review_user_count)      review_user_count,\n" +
            "       sum(good_review_user_count) good_review_user_count,\n" +
            "       good_review_user_count / review_user_count good_review_rate\n" +
            "from dws_interaction_course_review_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by course_id, course_name\n" +
            "order by good_review_user_count desc\n" +
            "limit 10;")
    List<CourseReviewStats> selectCourseReviewStats(@Param("date") Integer date);
}
