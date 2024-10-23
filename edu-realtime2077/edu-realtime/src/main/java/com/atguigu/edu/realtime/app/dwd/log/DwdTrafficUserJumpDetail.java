package com.atguigu.edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author yhm
 * @create 2023-04-21 17:54
 */
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境设置状态后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(4);

        // TODO 2 从kafka的page主题读取数据
        String topicName = "dwd_traffic_page_log";
        DataStreamSource<String> logDS = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, "dwd_traffic_user_jump_detail"), WatermarkStrategy.noWatermarks(), "user_jump_source");


        // 测试数据
        DataStream<String> kafkaDS = env
            .fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\"},\"ts\":15000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":30000} "
            );


        // TODO 3 过滤加转换数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // TODO 4 添加水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        // TODO 5 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = withWatermarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });

        // TODO 6 定义cep匹配规则
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(new IterativeCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject, Context<JSONObject> ctx) throws Exception {
                // 一个会话的开头   ->   last_page_id 为空
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null;
            }
        }).next("second").where(new IterativeCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject, Context<JSONObject> ctx) throws Exception {
                // 满足匹配的条件
                // 紧密相连  又一个会话的开头
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null;
            }
        }).within(Time.seconds(10L));


        // TODO 7 将CEP作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // TODO 8 提取匹配数据和超时数据
        OutputTag<String> timeoutTag = new OutputTag<String>("timeoutTag"){};
        SingleOutputStreamOperator<String> flatSelectStream = patternStream.flatSelect(timeoutTag, new PatternFlatTimeoutFunction<JSONObject, String>() {
            @Override
            public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                JSONObject first = pattern.get("first").get(0);
                out.collect(first.toJSONString());
            }
        }, new PatternFlatSelectFunction<JSONObject, String>() {
            @Override
            public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                JSONObject first = pattern.get("first").get(0);
                out.collect(first.toJSONString());
            }
        });

        SideOutputDataStream<String> timeoutStream = flatSelectStream.getSideOutput(timeoutTag);

        // TODO 9 合并数据写出到kafka
        DataStream<String> unionStream = flatSelectStream.union(timeoutStream);
        String targetTopic = "dwd_traffic_user_jump_detail";
        unionStream.sinkTo(KafkaUtil.getKafkaProducer(targetTopic,"user_jump_trans"));

        // TODO 10 执行任务
        env.execute();
    }
}
