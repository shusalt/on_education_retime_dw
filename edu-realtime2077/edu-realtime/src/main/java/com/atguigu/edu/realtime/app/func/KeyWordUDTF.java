package com.atguigu.edu.realtime.app.func;

import com.atguigu.edu.realtime.util.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;

/**
 * @author yhm
 * @create 2023-04-25 17:17
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeyWordUDTF  extends TableFunction<Row> {
    public void eval(String text) {
        ArrayList<String> analyze = KeyWordUtil.analyze(text);
        for (String s : analyze) {
            collect(Row.of(s));
        }
    }
}
