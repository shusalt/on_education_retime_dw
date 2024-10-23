package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwsLearnChapterPlayWindowBean;
import com.atguigu.edu.realtime.bean.DwsUserRegisterWindowBean;
import com.atguigu.edu.realtime.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author yhm
 * @create 2023-05-01 16:24
 */
public class DwsUserRegisterWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境设置状态后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        // TODO 2 读取dwd注册主题数据dwd_user_user_register
        String topicName = "dwd_user_user_register";
        String groupId = "dws_user_register_window";
        DataStreamSource<String> userRegisterSourceStream = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "user_register_source");

        // TODO 3 对null过滤 同时对user_id分组
        SingleOutputStreamOperator<JSONObject> notNullStream = userRegisterSourceStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    // 主要过滤掉回撤流的空数据
                    if (value != null) {
                        JSONObject jsonObject = JSON.parseObject(value);
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        KeyedStream<JSONObject, String> keyedStream = notNullStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("user_id");
            }
        });

        // TODO 4 使用状态对回撤流进行去重
        SingleOutputStreamOperator<JSONObject> processSteam = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<JSONObject> loginState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> loginStateDesc = new ValueStateDescriptor<>("login_state", JSONObject.class);
                // 设置过期时间
                loginStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(1L)).build());
                loginState = getRuntimeContext().getState(loginStateDesc);
            }

            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {

                JSONObject loginStateStr = loginState.value();
                if (loginStateStr == null) {
                    // 状态为空  说明是第一条数据
                    //启动定时器
                    ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 5000L);
                    loginState.update(jsonObj);
                } else {
                    String stateTs = loginStateStr.getString("row_op_ts");
                    String curTs = jsonObj.getString("row_op_ts");
                    // 如果当前的时间大于状态中的时间  说明当前的数据更加新一些
                    // 更新数据
                    if (TimestampLtz3CompareUtil.compare(curTs, stateTs) >= 0) {
                        loginState.update(jsonObj);
                    }
                }

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = loginState.value();
                out.collect(jsonObject);
            }
        });

        // TODO 5 转换为javaBean
        SingleOutputStreamOperator<DwsUserRegisterWindowBean> beanStream = processSteam.map(jsonObj -> DwsUserRegisterWindowBean.builder()
                .registerCount(1L)
                .ts(jsonObj.getLong("ts") * 1000)
                .build());

        // TODO 6 设置水位线
        SingleOutputStreamOperator<DwsUserRegisterWindowBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsUserRegisterWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsUserRegisterWindowBean>() {
            @Override
            public long extractTimestamp(DwsUserRegisterWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 7 开窗聚合
        SingleOutputStreamOperator<DwsUserRegisterWindowBean> reduceStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<DwsUserRegisterWindowBean>() {
                    @Override
                    public DwsUserRegisterWindowBean reduce(DwsUserRegisterWindowBean value1, DwsUserRegisterWindowBean value2) throws Exception {
                        value1.setRegisterCount(value1.getRegisterCount() + value2.getRegisterCount());
                        return value1;
                    }
                }, new ProcessAllWindowFunction<DwsUserRegisterWindowBean, DwsUserRegisterWindowBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsUserRegisterWindowBean> elements, Collector<DwsUserRegisterWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (DwsUserRegisterWindowBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                });

        // TODO 8 写出到clickHouse
        reduceStream.addSink(ClickHouseUtil.getJdbcSink("" +
                "insert into dws_user_register_window values(?,?,?,?)"));

        // TODO 9 执行任务
        env.execute();
    }
}
