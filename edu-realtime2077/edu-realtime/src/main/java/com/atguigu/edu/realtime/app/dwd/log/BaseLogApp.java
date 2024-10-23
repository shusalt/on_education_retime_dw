package com.atguigu.edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author yhm
 * @create 2023-04-21 14:01
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建环境设置状态后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        //TODO 2 从kafka中读取主流数据
        String topicName = "topic_log";
        String groupId = "base_log_app";
        DataStreamSource<String> baseLogSource = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId),
                WatermarkStrategy.noWatermarks(),
                "base_log_source"
        );

        //TODO 3 对数据进行清洗转换
        // 3.1 定义侧输出流
        OutputTag<String> dirtyStreamTag = new OutputTag<String>("dirtyStream"){};

        // 3.2 清洗转换
        SingleOutputStreamOperator<JSONObject> cleanedStream = baseLogSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyStreamTag, value);
                }
            }
        });
        // 3.3 将脏数据写出到kafka对应的主题
        SideOutputDataStream<String> dirtyStream = cleanedStream.getSideOutput(dirtyStreamTag);
        String dirtyTopicName = "dirty_data";
        dirtyStream.sinkTo(KafkaUtil.getKafkaProducer(dirtyTopicName,"dirty_trans"));

        //TODO 4 新老访客标记修复
        // 4.1 安装mid对数据进行分组
        KeyedStream<JSONObject, String> keyedStream = cleanedStream.keyBy(r -> r.getJSONObject("common").getString("mid"));

        // 4.2 修复新旧访客
        SingleOutputStreamOperator<JSONObject> fixedStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            ValueState<String> firstLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                firstLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstLoginDt", String.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, Context ctx, Collector<JSONObject> out) throws Exception {
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                String firstLoginDt = firstLoginDtState.value();
                Long ts = jsonObject.getLong("ts");
                String nowLoginDt = DateFormatUtil.toDate(ts);
                // 日志标记为新访客
                if ("1".equals(isNew)) {
                    if (firstLoginDt == null) {
                        // 状态为空 真的是新访客
                        firstLoginDtState.update(nowLoginDt);
                    } else {
                        // 状态不为空
                        if (!firstLoginDt.equals(nowLoginDt)) {
                            // 日期不相等
                            // 骗人的不是新访客
                            isNew = "0";
                            jsonObject.getJSONObject("common").put("is_new", isNew);
                        }
                    }
                } else {
                    // 标记为旧访客
                    if (firstLoginDt == null) {
                        // 补全状态
                        String yesterday = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000);
                        firstLoginDtState.update(yesterday);
                    }
                }
                out.collect(jsonObject);
            }
        });

        //TODO 5 数据分流
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
        OutputTag<String> errorTag = new OutputTag<String>("errorTag") {};
        OutputTag<String> appVideoTag = new OutputTag<String>("appVideoTag") {};
//        fixedStream.print();

        SingleOutputStreamOperator<String> pageStream = fixedStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context ctx, Collector<String> out) throws Exception {
                // 5.1 错误数据
                JSONObject errJsonObj = jsonObject.getJSONObject("err");
                if (errJsonObj != null) {
                    ctx.output(errorTag, errJsonObj.toJSONString());
                }

                jsonObject.remove("err");

                // 5.2 启动日志
                JSONObject startJsonObj = jsonObject.getJSONObject("start");
                if (startJsonObj != null) {
                    ctx.output(startTag, jsonObject.toJSONString());
                }
                // 不是启动日志
                // 判断播放日志
                else if (jsonObject.getJSONObject("appVideo") != null) {
                    // 5.3 播放日志
                    ctx.output(appVideoTag, jsonObject.toJSONString());

                } else {
                    // 页面日志
                    // 5.4 曝光日志
                    JSONObject pageJsonObj = jsonObject.getJSONObject("page");
                    JSONObject commonJsonObj = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");

                    JSONArray displaysJsonArray = jsonObject.getJSONArray("displays");
                    if (displaysJsonArray != null) {
                        for (int i = 0; i < displaysJsonArray.size(); i++) {
                            JSONObject displayJsonObj = displaysJsonArray.getJSONObject(i);
                            JSONObject result = new JSONObject();
                            result.put("display", displayJsonObj);
                            result.put("common", commonJsonObj);
                            result.put("page", pageJsonObj);
                            result.put("ts", ts);
                            ctx.output(displayTag, result.toJSONString());
                        }
                    }

                    // 5.5 动作日志
                    JSONArray actionsJsonArray = jsonObject.getJSONArray("actions");
                    if (actionsJsonArray != null) {
                        for (int i = 0; i < actionsJsonArray.size(); i++) {
                            JSONObject actionJsonObj = actionsJsonArray.getJSONObject(i);
                            JSONObject result = new JSONObject();
                            result.put("action", actionJsonObj);
                            result.put("common", commonJsonObj);
                            result.put("page", pageJsonObj);
                            result.put("ts", ts);
                            ctx.output(actionTag, result.toJSONString());
                        }
                    }
                    jsonObject.remove("actions");
                    jsonObject.remove("displays");
                    out.collect(jsonObject.toJSONString());
                }
            }
        });


        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        SideOutputDataStream<String> errorStream = pageStream.getSideOutput(errorTag);
        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> appVideoStream = pageStream.getSideOutput(appVideoTag);


        //TODO 6 写出到kafka不同的主题
        String pageTopic = "dwd_traffic_page_log";
        String startTopic = "dwd_traffic_start_log";
        String displayTopic = "dwd_traffic_display_log";
        String actionTopic = "dwd_traffic_action_log";
        String errorTopic = "dwd_traffic_error_log";
        String appVideoTopic = "dwd_traffic_play_pre_process";

        pageStream.sinkTo(KafkaUtil.getKafkaProducer(pageTopic,"page_trans"));
        startStream.sinkTo(KafkaUtil.getKafkaProducer(startTopic,"start_trans"));
        appVideoStream.sinkTo(KafkaUtil.getKafkaProducer(appVideoTopic,"appVideo_trans"));
        displayStream.sinkTo(KafkaUtil.getKafkaProducer(displayTopic,"display_trans"));
        actionStream.sinkTo(KafkaUtil.getKafkaProducer(actionTopic,"action_trans"));
        errorStream.sinkTo(KafkaUtil.getKafkaProducer(errorTopic,"error_trans"));


        //TODO 7 执行任务
        env.execute();
    }
}
