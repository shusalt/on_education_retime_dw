package com.atguigu.edu.publisher.controller;

import com.atguigu.edu.publisher.bean.VedioChapterStats;
import com.atguigu.edu.publisher.service.VideoChapterStatsService;
import com.atguigu.edu.publisher.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/edu/realtime/video")
public class VideoStatsController {

    @Autowired
    private VideoChapterStatsService videoChapterStatsService;

    @RequestMapping("/playStatsPerChapter")
    public String getVedioChapterStats(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<VedioChapterStats> vedioChapterStatsList = videoChapterStatsService.getVedioChapterStats(date);

        if (vedioChapterStatsList  == null) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < vedioChapterStatsList .size(); i++) {
            VedioChapterStats vedioChapterStats = vedioChapterStatsList.get(i);
            String chapterName = vedioChapterStats.getChapterName();
            Integer playCT = vedioChapterStats.getPlayCT();
            Double playTotalSec = vedioChapterStats.getPlayTotalSec();
            Integer uuCt = vedioChapterStats.getUuCt();
            Double secPerUser = vedioChapterStats.getSecPerUser();


            rows.append("{\n" +
                    "\t\"chapterName\": \"" + chapterName + "\",\n" +
                    "\t\"playCT\": \"" + playCT  + "\",\n" +
                    "\t\"playTotalSec\": \"" + playTotalSec + "\",\n" +
                    "\t\"uuCt\": \"" + uuCt + "\",\n" +
                    "\t\"secPerUser\": \"" + secPerUser + "\"\n" +
                    "}");
            if (i < vedioChapterStatsList.size() - 1) {
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
                "        \"name\": \"章节名称\",\n" +
                "        \"id\": \"chapterName\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"播放次数\",\n" +
                "        \"id\": \"playCT\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"播放时长\",\n" +
                "        \"id\": \"playTotalSec\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"观看人数\",\n" +
                "        \"id\": \"uuCt\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"人均观看时长\",\n" +
                "        \"id\": \"secPerUser\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }
}
