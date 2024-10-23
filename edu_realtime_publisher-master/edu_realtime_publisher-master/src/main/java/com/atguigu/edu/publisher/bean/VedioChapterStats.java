package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class VedioChapterStats {

    //章节名称
    String chapterName;

    //播放次数
    Integer playCT;

    //播放时长
    Double playTotalSec;

    //观看人数
    Integer uuCt;

    //人均观看时长
    Double secPerUser;
}
