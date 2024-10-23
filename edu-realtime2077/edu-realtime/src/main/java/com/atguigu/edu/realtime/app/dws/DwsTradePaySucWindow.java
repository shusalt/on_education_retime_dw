package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwsTradeOrderWindowBean;
import com.atguigu.edu.realtime.bean.DwsTradePaySucWindowBean;
import com.atguigu.edu.realtime.bean.DwsTradePaySucWindowBean;
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
 * @create 2023-05-02 8:34
 */
public class DwsTradePaySucWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境设置后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        // TODO 2 读取dwd层支付成功主题数据dwd_trade_pay_suc_detail
        String topicName = "dwd_trade_pay_suc_detail";
        String groupId = "dws_trade_pay_suc_window";
        DataStreamSource<String> paySucSource = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "pay_suc_source");

        // TODO 3 过滤null值转换数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjStream = paySucSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    if (value != null) {
                        JSONObject jsonObject = JSON.parseObject(value);
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // TODO 4 按照订单明细ID分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("id");
            }
        });
        // TODO 5 使用flink状态去重
        SingleOutputStreamOperator<JSONObject> processStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<JSONObject> payState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> payStateDesc = new ValueStateDescriptor<>("pay_state", JSONObject.class);
                // 设置过期时间
                payStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(1L)).build());
                payState = getRuntimeContext().getState(payStateDesc);
            }

            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {

                JSONObject orderStateObj = payState.value();
                if (orderStateObj == null) {
                    // 状态为空  说明是第一条数据
                    //启动定时器
                    ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 5000L);
                    payState.update(jsonObj);
                } else {
                    String stateTs = orderStateObj.getString("row_op_ts");
                    String curTs = jsonObj.getString("row_op_ts");
                    // 如果当前的时间大于状态中的时间  说明当前的数据更加新一些
                    // 更新数据
                    if (TimestampLtz3CompareUtil.compare(curTs, stateTs) >= 0) {
                        payState.update(jsonObj);
                    }
                }

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                // 5S之后启动定时器  将数据写出
                JSONObject jsonObject = payState.value();
                out.collect(jsonObject);
            }
        });


        // TODO 6 按照user_id分组
        KeyedStream<JSONObject, String> userIdKeyedStream = processStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("user_id");
            }
        });

        // TODO 7 判断是否为新用户或者当天独立用户
        SingleOutputStreamOperator<DwsTradePaySucWindowBean> uvCountStream = userIdKeyedStream.process(new KeyedProcessFunction<String, JSONObject, DwsTradePaySucWindowBean>() {
            ValueState<String> lastPayDtState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastPayDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_pay_dt_state", String.class));
            }

            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<DwsTradePaySucWindowBean> out) throws Exception {
                long ts = jsonObj.getLong("ts") * 1000;
                String curDate = DateFormatUtil.toDate(ts);
                String lastOrderDt = lastPayDtState.value();
                long payUvCount = 0L;
                long newPayUserCount = 0L;
                if (lastOrderDt == null) {
                    // 是新用户
                    payUvCount = 1L;
                    newPayUserCount = 1L;
                } else if (lastOrderDt.compareTo(curDate) < 0) {

                    payUvCount = 1L;
                }
                // 判断是独立用户才需要往下游传递
                if (payUvCount != 0) {
                    out.collect(DwsTradePaySucWindowBean.builder()
                            .paySucUvCount(payUvCount)
                            .paySucNewUserCount(newPayUserCount)
                            .ts(ts)
                            .build());
                }
            }
        });

        // TODO 8 设置水位线
        SingleOutputStreamOperator<DwsTradePaySucWindowBean> withWaterMarkStream = uvCountStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTradePaySucWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsTradePaySucWindowBean>() {
            @Override
            public long extractTimestamp(DwsTradePaySucWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 9 开窗聚合
        SingleOutputStreamOperator<DwsTradePaySucWindowBean> reduceStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<DwsTradePaySucWindowBean>() {
                    @Override
                    public DwsTradePaySucWindowBean reduce(DwsTradePaySucWindowBean value1, DwsTradePaySucWindowBean value2) throws Exception {
                        value1.setPaySucUvCount(value1.getPaySucUvCount() + value2.getPaySucUvCount());
                        value1.setPaySucNewUserCount(value1.getPaySucNewUserCount() + value2.getPaySucNewUserCount());
                        return value1;
                    }
                }, new ProcessAllWindowFunction<DwsTradePaySucWindowBean, DwsTradePaySucWindowBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsTradePaySucWindowBean> elements, Collector<DwsTradePaySucWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (DwsTradePaySucWindowBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                });

        // TODO 10 写出到clickHouse
        reduceStream.addSink(ClickHouseUtil.getJdbcSink("" +
                "insert into dws_trade_pay_suc_window values(?,?,?,?,?)"));

        // TODO 11 执行任务
        env.execute();
    }
}
