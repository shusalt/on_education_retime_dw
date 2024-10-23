package com.atguigu.edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwdUserUserLoginBean;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author yhm
 * @create 2023-04-23 16:02
 */
public class DwdUserUserLogin {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建环境设置状态后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        //TODO 2 读取kafka的dwd_traffic_page_log主题数据
        String topicName = "dwd_traffic_page_log";
        String groupId = "dwd_user_user_login";
        DataStreamSource<String> pageStream = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "user_login");

        //TODO 3 过滤及转换
        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    if (jsonObject.getJSONObject("common").getString("uid") != null) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });


        //TODO 4 添加水位线
        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //TODO 5 按照会话id分组
        KeyedStream<JSONObject, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        //TODO 6 使用状态找出每个会话第一条数据
        SingleOutputStreamOperator<JSONObject> firstStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<JSONObject> firstLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("first_login_dt", JSONObject.class);

                // 添加状态存活时间
                valueStateDescriptor.enableTimeToLive(StateTtlConfig
                        .newBuilder(Time.days(1L))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());

                firstLoginDtState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, Context ctx, Collector<JSONObject> out) throws Exception {
                // 处理数据
                // 获取状态
                JSONObject firstLoginDt = firstLoginDtState.value();
                Long ts = jsonObject.getLong("ts");
                if (firstLoginDt == null) {
                    firstLoginDtState.update(jsonObject);
                    // 第一条数据到的时候开启定时器
                    ctx.timerService().registerEventTimeTimer(ts + 10 * 1000L);
                } else {
                    Long lastTs = firstLoginDt.getLong("ts");
                    if (ts < lastTs) {
                        firstLoginDtState.update(jsonObject);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                out.collect(firstLoginDtState.value());
            }
        });

        //TODO 7 转换结构
        SingleOutputStreamOperator<String> mapStream = firstStream.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject jsonObj) throws Exception {
                JSONObject common = jsonObj.getJSONObject("common");
                Long ts = jsonObj.getLong("ts");
                String loginTime = DateFormatUtil.toYmdHms(ts);
                String dateId = loginTime.substring(0, 10);

                DwdUserUserLoginBean dwdUserUserLoginBean = DwdUserUserLoginBean.builder()
                        .userId(common.getString("uid"))
                        .dateId(dateId)
                        .loginTime(loginTime)
                        .channel(common.getString("ch"))
                        .provinceId(common.getString("ar"))
                        .versionCode(common.getString("vc"))
                        .midId(common.getString("mid"))
                        .brand(common.getString("ba"))
                        .model(common.getString("md"))
                        .sourceId(common.getString("sc"))
                        .operatingSystem(common.getString("os"))
                        .ts(ts)
                        .build();

                return JSON.toJSONString(dwdUserUserLoginBean);
            }
        });

        //TODO 8 输出数据
        String sinkTopic = "dwd_user_user_login";
        mapStream.sinkTo(KafkaUtil.getKafkaProducer(sinkTopic,"user_login_trans"));

        //TODO 9 执行任务
        env.execute();
    }
}
