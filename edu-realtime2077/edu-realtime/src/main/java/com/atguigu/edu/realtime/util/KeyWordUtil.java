package com.atguigu.edu.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

/**
 * @author yhm
 * @create 2023-04-25 16:05
 */
public class KeyWordUtil {

    public static ArrayList<String> analyze(String text){
        StringReader reader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
        ArrayList<String> strings = new ArrayList<>();
        try {
            Lexeme lexeme = null;
            while ((lexeme = ikSegmenter.next())!=null){
                String keyWord = lexeme.getLexemeText();
                strings.add(keyWord);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return strings;
    }

    public static void main(String[] args) {
        String s = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待";
        ArrayList<String> strings = analyze(s);
        System.out.println(strings);
    }
}
