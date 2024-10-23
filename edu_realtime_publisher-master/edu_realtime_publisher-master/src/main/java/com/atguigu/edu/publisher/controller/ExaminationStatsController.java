package com.atguigu.edu.publisher.controller;


import com.atguigu.edu.publisher.bean.*;
import com.atguigu.edu.publisher.service.ExaminationStatsService;
import com.atguigu.edu.publisher.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/edu/realtime/examination")
public class ExaminationStatsController {

    @Autowired
    private ExaminationStatsService examinationStatsService;

    @RequestMapping("/paperStats")
    public String getExaminationPaperCt(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }

        List<ExaminationPaperStats> examinationPaperStatsList = examinationStatsService.getExaminationPaperStats(date);
        if (examinationPaperStatsList == null || examinationPaperStatsList.size() == 0) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < examinationPaperStatsList.size(); i++) {
            ExaminationPaperStats examinationPaperStats = examinationPaperStatsList.get(i);
            String paperTitle = examinationPaperStats.getPaperTitle();
            Long examTakenCount = examinationPaperStats.getExamTakenCount();
            Double avgScore = examinationPaperStats.getAvgScore();
            Double avgSec = examinationPaperStats.getAvgSec();


            rows.append("{\n" +
                    "\t\"paperTitle\": \"" + paperTitle + "\",\n" +
                    "\t\"examTakenCount\": \"" + examTakenCount + "\",\n" +
                    "\t\"avgScore\": \"" + avgScore + "\",\n" +
                    "\t\"avgSec\": \"" + avgSec + "\"\n" +
                    "}");
            if (i < examinationPaperStatsList.size() - 1) {
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
                "        \"name\": \"试卷名称\",\n" +
                "        \"id\": \"paperName\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"考试人次\",\n" +
                "        \"id\": \"examTakenCount\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"平均分\",\n" +
                "        \"id\": \"avgScore\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"平均用时\",\n" +
                "        \"id\": \"avgSec\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }

    @RequestMapping("/courseStats")
    public String getExaminationCourseCt(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<ExaminationCourseStats> examinationCourseStatsList = examinationStatsService.getExaminationCourseStats(date);

        if (examinationCourseStatsList == null || examinationCourseStatsList.size() == 0) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < examinationCourseStatsList.size(); i++) {
            ExaminationCourseStats examinationCourseStats = examinationCourseStatsList.get(i);
            String courseName = examinationCourseStats.getCourseName();
            Long examTakenCount = examinationCourseStats.getExamTakenCount();
            Double avgScore = examinationCourseStats.getAvgScore();
            Double avgSec = examinationCourseStats.getAvgSec();


            rows.append("{\n" +
                    "\t\"courseName\": \"" + courseName + "\",\n" +
                    "\t\"examTakenCount\": \"" + examTakenCount + "\",\n" +
                    "\t\"avgScore\": \"" + avgScore + "\",\n" +
                    "\t\"avgSec\": \"" + avgSec + "\"\n" +
                    "}");
            if (i < examinationCourseStatsList.size() - 1) {
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
                "        \"name\": \"课程名称\",\n" +
                "        \"id\": \"courseName\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"考试人次\",\n" +
                "        \"id\": \"examTakenCount\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"平均分\",\n" +
                "        \"id\": \"avgScore\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"平均用时\",\n" +
                "        \"id\": \"avgSec\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }

    @RequestMapping("/scoreDurDistribution")
    public String getScoreDurDistribution(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }

        List<ExaminationScoreDurDistribution> examinationScoreDurDistributionList = examinationStatsService.getExaminationScoreDurDistribution(date);

        if (examinationScoreDurDistributionList == null || examinationScoreDurDistributionList.size() == 0) {
            return "";
        }
        StringBuilder data = new StringBuilder("[");

        for (int i = 0; i < examinationScoreDurDistributionList.size(); i++) {
            ExaminationScoreDurDistribution examinationScoreDurDistribution = examinationScoreDurDistributionList.get(i);
            String scoreDuration = examinationScoreDurDistribution.getScoreDuration();
            Long userCout = examinationScoreDurDistribution.getUserCout();

            data.append("{\n" +
                    "      \"name\": \"" + scoreDuration + "\",\n" +
                    "      \"value\": " + userCout + "\n" +
                    "    }");

            if (i < examinationScoreDurDistributionList.size() - 1) {
                data.append(",");
            } else {
                data.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": " + data + "\n" +
                "}";
    }

    @RequestMapping("/questionStats")
    public String getExaminationQuestionStats(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<ExaminationQuestionStats> examinationQuestionStatsList = examinationStatsService.getExaminationQuestionStats(date);

        if (examinationQuestionStatsList == null || examinationQuestionStatsList.size() == 0) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < examinationQuestionStatsList.size(); i++) {
            ExaminationQuestionStats examinationQuetstionCt = examinationQuestionStatsList.get(i);

            String questionTxt = examinationQuetstionCt.processQuestionTxt();
            Long answerCt = examinationQuetstionCt.getAnswerCt();
            Long correctAnswerCt = examinationQuetstionCt.getCorrectAnswerCt();
            Double accuracyRate = examinationQuetstionCt.getAccuracyRate();

            rows.append("{\n" +
                    "\t\"questionTxt\": \"" + questionTxt + "\",\n" +
                    "\t\"answerCt\": \"" + answerCt + "\",\n" +
                    "\t\"correctAnswerCt\": \"" + correctAnswerCt + "\",\n" +
                    "\t\"accuracyRate\": \"" + accuracyRate + "\"\n" +
                    "}");
            if (i < examinationQuestionStatsList.size() - 1) {
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
                "        \"name\": \"题目内容\",\n" +
                "        \"id\": \"questionTxt\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"答题次数\",\n" +
                "        \"id\": \"answerCt\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"正确答题次数\",\n" +
                "        \"id\": \"correctAnswerCt\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"正确率\",\n" +
                "        \"id\": \"accuracyRate\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }
}
