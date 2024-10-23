package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.DigestUtils;

import java.nio.charset.StandardCharsets;

@Data
@AllArgsConstructor
public class ExaminationQuestionStats {

    // 题目
    String questionTxt;

    // 正确次数
    Long correctAnswerCt;

    // 答题次数
    Long answerCt;

    // 正确率
    Double accuracyRate;

    public String processQuestionTxt(){
        String md5 = DigestUtils.md5DigestAsHex(questionTxt.getBytes(StandardCharsets.UTF_8));
        return md5.substring(0, 10);
    }
}
