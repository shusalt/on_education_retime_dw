package com.atguigu.edu.publisher.controller;

import com.atguigu.edu.publisher.bean.*;
import com.atguigu.edu.publisher.service.CourseStatsService;
import com.atguigu.edu.publisher.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/edu/realtime/course")
public class CourseStatsController {

    @Autowired
    private CourseStatsService courseStatsService;

    @RequestMapping("/courseOrderStats")
    public String getCourseOrderStats(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<CourseOrderStats> courseOrderStats = courseStatsService.getCourseOrderStats(date);
        if (courseOrderStats == null || courseOrderStats.size() == 0) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < courseOrderStats.size(); i++) {
            CourseOrderStats courseOrderStat = courseOrderStats.get(i);
            String courseName = courseOrderStat.getCourseName();
            BigDecimal orderTotalAmount = courseOrderStat.getOrderTotalAmount();


            rows.append("{\n" +
                    "\t\"course_name\": \"" + courseName + "\",\n" +
                    "\t\"order_total_amount\": \"" + orderTotalAmount + "\"\n" +
                    "}");
            if (i < courseOrderStats.size() - 1) {
                rows.append(",");
            } else {
                rows.append("]");
            }
        }

        String str = "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"columns\": [\n" +
                "      {\n" +
                "        \"name\": \"课程名称\",\n" +
                "        \"id\": \"course_name\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"订单金额\",\n" +
                "        \"id\": \"order_total_amount\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
        String finalStr = str.replaceAll("\\\\", ",");
        return finalStr;
    }

    @RequestMapping("/subjectOrderStats")
    public String getSubjectOrderStats(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<SubjectOrderStats> subjectOrderStats = courseStatsService.getSubjectOrderStats(date);

        if (subjectOrderStats == null || subjectOrderStats.size() == 0) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < subjectOrderStats.size(); i++) {
            SubjectOrderStats subjectOrderStat = subjectOrderStats.get(i);

            String subjectName = subjectOrderStat.getSubjectName();

            BigDecimal orderTotalAmount = subjectOrderStat.getOrderTotalAmount();

            rows.append("{\n" +
                    "      \"subject_name\": \"" + subjectName + "\",\n" +
                    "      \"order_total_amount\": " + orderTotalAmount + "\n" +
                    "    }");
            if (i < subjectOrderStats.size() - 1) {
                rows.append(",");
            } else {
                rows.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"columns\": [\n" +
                "      {\n" +
                "        \"name\": \"科目名称\",\n" +
                "        \"id\": \"subject_name\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"订单金额\",\n" +
                "        \"id\": \"order_total_amount\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }

    @RequestMapping("/categoryOrderStats")
    public String getCategoryOrderStats(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<Category0rderStats> categoryOrderStats = courseStatsService.getCategoryOrderStats(date);

        if (categoryOrderStats == null || categoryOrderStats.size() == 0) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < categoryOrderStats.size(); i++) {
            Category0rderStats category0rderStat = categoryOrderStats.get(i);

            String categoryName = category0rderStat.getCategoryName();

            BigDecimal orderTotalAmount = category0rderStat.getOrderTotalAmount();

            rows.append("{\n" +
                    "      \"category_name\": \"" + categoryName + "\",\n" +
                    "      \"order_total_amount\": " + orderTotalAmount + "\n" +
                    "    }");
            if (i < categoryOrderStats.size() - 1) {
                rows.append(",");
            } else {
                rows.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"columns\": [\n" +
                "      {\n" +
                "        \"name\": \"类别名称\",\n" +
                "        \"id\": \"category_name\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"订单金额\",\n" +
                "        \"id\": \"order_total_amount\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }

@RequestMapping("/courseReviewStats")
public String getCourseReviewStats(
        @RequestParam(value = "date", defaultValue = "1") Integer date) {
    if (date == 1) {
        date = DateUtil.now();
    }
    List<CourseReviewStats> courseReviewStats = courseStatsService.getCourseReviewStats(date);

    if (courseReviewStats == null || courseReviewStats.size() == 0) {
        return "";
    }
    StringBuilder rows = new StringBuilder("[");
    for (int i = 0; i < courseReviewStats.size(); i++) {
        CourseReviewStats courseReviewStat = courseReviewStats.get(i);

        String courseName = courseReviewStat.getCourseName();

        Double avgStars = courseReviewStat.getAvgStars();
        Integer reviewUserCt = courseReviewStat.getReviewUserCt();
        Integer goodReviewUserCt = courseReviewStat.getGoodReviewUserCt();
        Double goodReviewRate = courseReviewStat.getGoodReviewRate();

        rows.append("{\n" +
                "\t\"course_name\": \"" + courseName + "\",\n" +
                "\t\"avg_stars\": \"" + avgStars + "\",\n" +
                "\t\"review_user_ct\": \"" + reviewUserCt + "\",\n" +
                "\t\"good_review_user_ct\": \"" + goodReviewUserCt + "\",\n" +
                "\t\"good_review_rate\": \"" + goodReviewRate + "\"\n" +
                "}");
        if (i < courseReviewStats.size() - 1) {
            rows.append(",");
        } else {
            rows.append("]");
        }
    }
    String str = "{\n" +
            "  \"status\": 0,\n" +
            "  \"msg\": \"\",\n" +
            "  \"data\": {\n" +
            "    \"columns\": [\n" +
            "      {\n" +
            "        \"name\": \"课程名称\",\n" +
            "        \"id\": \"course_name\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"name\": \"平均评分\",\n" +
            "        \"id\": \"avg_stars\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"name\": \"评价人数\",\n" +
            "        \"id\": \"review_user_ct\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"name\": \"好评人数\",\n" +
            "        \"id\": \"good_review_user_ct\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"name\": \"好评率\",\n" +
            "        \"id\": \"good_review_rate\"\n" +
            "      }\n" +
            "    ],\n" +
            "    \"rows\": " + rows + "\n" +
            "  }\n" +
            "}";
    String finalStr = str.replaceAll("\\\\", ",");

    return finalStr;
}
}
