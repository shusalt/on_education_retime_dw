package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwsTradeOrderWindowBean;
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
 * @create 2023-05-01 21:34
 */
public class DwsTradeOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境设置后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        // TODO 2 读取dwd下单明细主题数据dwd_trade_order_detail
        String topicName = "dwd_trade_order_detail";
        String groupId = "dws_trade_order_window";
        DataStreamSource<String> orderDetailSource = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "order_detail_source");

        // TODO 3 过滤null数据转换数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjStream = orderDetailSource.flatMap(new FlatMapFunction<String, JSONObject>() {
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

        // TODO 4 按照订单详情id分组去重
        // 找到回撤流完整的数据
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("id");
            }
        });
        SingleOutputStreamOperator<JSONObject> processStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<JSONObject> orderState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> orderStateDesc = new ValueStateDescriptor<>("order_state", JSONObject.class);
                // 设置过期时间
                orderStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(1L)).build());
                orderState = getRuntimeContext().getState(orderStateDesc);
            }

            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {

                JSONObject orderStateObj = orderState.value();
                if (orderStateObj == null) {
                    // 状态为空  说明是第一条数据
                    //启动定时器
                    ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 5000L);
                    orderState.update(jsonObj);
                } else {
                    String stateTs = orderStateObj.getString("row_op_ts");
                    String curTs = jsonObj.getString("row_op_ts");
                    // 如果当前的时间大于状态中的时间  说明当前的数据更加新一些
                    // 更新数据
                    if (TimestampLtz3CompareUtil.compare(curTs, stateTs) >= 0) {
                        orderState.update(jsonObj);
                    }
                }

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = orderState.value();
                out.collect(jsonObject);
            }
        });

        // TODO 5 按照user_id分组
        KeyedStream<JSONObject, String> userIdKeyedStream = processStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("user_id");
            }
        });

        // TODO 6 使用状态判断是否为独立用户 或 新用户
        SingleOutputStreamOperator<DwsTradeOrderWindowBean> uvCountStream = userIdKeyedStream.process(new KeyedProcessFunction<String, JSONObject, DwsTradeOrderWindowBean>() {
            ValueState<String> lastOrderDtState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastOrderDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_order_dt_state", String.class));
            }

            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<DwsTradeOrderWindowBean> out) throws Exception {
                long ts = jsonObj.getLong("ts") * 1000;
                String curDate = DateFormatUtil.toDate(ts);
                String lastOrderDt = lastOrderDtState.value();
                long orderUvCount = 0L;
                long newOrderUserCount = 0L;
                if (lastOrderDt == null) {
                    // 是新用户
                    orderUvCount = 1L;
                    newOrderUserCount = 1L;
                } else if (lastOrderDt.compareTo(curDate) < 0) {

                    orderUvCount = 1L;
                }
                // 判断是独立用户才需要往下游传递
                if (orderUvCount != 0) {
                    out.collect(DwsTradeOrderWindowBean.builder()
                            .orderUvCount(orderUvCount)
                            .newOrderUserCount(newOrderUserCount)
                            .ts(ts)
                            .build());
                }
            }
        });

        // TODO 7 添加水位线
        SingleOutputStreamOperator<DwsTradeOrderWindowBean> withWaterMarkStream = uvCountStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTradeOrderWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeOrderWindowBean>() {
            @Override
            public long extractTimestamp(DwsTradeOrderWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 8 开窗聚合
        SingleOutputStreamOperator<DwsTradeOrderWindowBean> reduceStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<DwsTradeOrderWindowBean>() {
                    @Override
                    public DwsTradeOrderWindowBean reduce(DwsTradeOrderWindowBean value1, DwsTradeOrderWindowBean value2) throws Exception {
                        value1.setOrderUvCount(value1.getOrderUvCount() + value2.getOrderUvCount());
                        value1.setNewOrderUserCount(value1.getNewOrderUserCount() + value2.getNewOrderUserCount());
                        return value1;
                    }
                }, new ProcessAllWindowFunction<DwsTradeOrderWindowBean, DwsTradeOrderWindowBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsTradeOrderWindowBean> elements, Collector<DwsTradeOrderWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (DwsTradeOrderWindowBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                });

        // TODO 9 写出到clickHouse
        reduceStream.addSink(ClickHouseUtil.getJdbcSink("" +
                "insert into dws_trade_order_window values(?,?,?,?,?)"));

        // TODO 10 执行任务
        env.execute();
    }
}
