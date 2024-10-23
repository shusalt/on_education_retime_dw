package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsTradeOrderWindowBean;
import com.atguigu.edu.realtime.bean.DwsTradeSourceOrderWindowBean;
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
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author yhm
 * @create 2023-05-02 10:31
 */
public class DwsTradeSourceOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境设置后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        // TODO 2 读取dwd下单明细主题数据dwd_trade_order_detail
        String topicName = "dwd_trade_order_detail";
        String groupId = "dws_trade_source_order_window";
        DataStreamSource<String> orderDetailSource = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "order_detail_source");

        // TODO 3 过滤null数据转换数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjStream = orderDetailSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    if (value != null) {
                        JSONObject jsonObject = JSON.parseObject(value);
                        if (jsonObject.getString("source_id") != null && jsonObject.getString("order_id") != null){
                            out.collect(jsonObject);
                        }
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

        // TODO 5 按照来源id和用户id分组
        KeyedStream<JSONObject, String> userIdSourceIdStream = processStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObj) throws Exception {
                return jsonObj.getString("user_id") + jsonObj.getString("source_id");
            }
        });

        // TODO 6 统计独立用户
        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> uuBeanStream = userIdSourceIdStream.process(new KeyedProcessFunction<String, JSONObject, DwsTradeSourceOrderWindowBean>() {

            ValueState<String> lastOrderDtState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastOrderDtStateDesc = new ValueStateDescriptor<>("last_order_dt_state", String.class);
                lastOrderDtStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastOrderDtState = getRuntimeContext().getState(lastOrderDtStateDesc);
            }

            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<DwsTradeSourceOrderWindowBean> out) throws Exception {
                long ts = jsonObj.getLong("ts") * 1000;
                String lastOrderDt = lastOrderDtState.value();
                String curDt = DateFormatUtil.toDate(ts);
                Long orderUuCount = 0L;
                if (lastOrderDt == null || lastOrderDt.compareTo(curDt) < 0) {
                    // 判断为独立用户
                    orderUuCount = 1L;
                    lastOrderDtState.update(curDt);
                }
                out.collect(DwsTradeSourceOrderWindowBean.builder()
                        .sourceId(jsonObj.getString("source_id"))
                        .orderTotalAmount(new BigDecimal(jsonObj.getString("final_amount")))
                        .orderUuCount(orderUuCount)
                        .orderId(jsonObj.getString("order_id"))
                        .ts(ts)
                        .build());
            }
        });

        // TODO 7 按照订单id分组
        KeyedStream<DwsTradeSourceOrderWindowBean, String> orderIdStream = uuBeanStream.keyBy(new KeySelector<DwsTradeSourceOrderWindowBean, String>() {
            @Override
            public String getKey(DwsTradeSourceOrderWindowBean value) throws Exception {
                return value.getOrderId();
            }
        });

        // TODO 8 统计订单数
        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> orderBeanStream = orderIdStream.process(new KeyedProcessFunction<String, DwsTradeSourceOrderWindowBean, DwsTradeSourceOrderWindowBean>() {

            ValueState<String> lastOrderIdState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastOrderStateDesc = new ValueStateDescriptor<>("last_order_state", String.class);
                lastOrderStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(1)).build());
                lastOrderIdState = getRuntimeContext().getState(lastOrderStateDesc);
            }

            @Override
            public void processElement(DwsTradeSourceOrderWindowBean bean, Context ctx, Collector<DwsTradeSourceOrderWindowBean> out) throws Exception {
                String lastOrderId = lastOrderIdState.value();
                Long orderCount = 0L;
                String orderId = bean.getOrderId();
                if (lastOrderId == null) {
                    orderCount = 1L;
                    lastOrderIdState.update(orderId);
                }
                bean.setOrderCount(orderCount);
                out.collect(bean);
            }
        });

        // TODO 9 设置水位线
        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> withWaterMarkStream = orderBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTradeSourceOrderWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeSourceOrderWindowBean>() {
            @Override
            public long extractTimestamp(DwsTradeSourceOrderWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 10 分组开窗聚合
        KeyedStream<DwsTradeSourceOrderWindowBean, String> sourceIdStream = withWaterMarkStream.keyBy(new KeySelector<DwsTradeSourceOrderWindowBean, String>() {
            @Override
            public String getKey(DwsTradeSourceOrderWindowBean value) throws Exception {
                return value.getSourceId();
            }
        });

        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> reduceStream = sourceIdStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<DwsTradeSourceOrderWindowBean>() {
                    @Override
                    public DwsTradeSourceOrderWindowBean reduce(DwsTradeSourceOrderWindowBean value1, DwsTradeSourceOrderWindowBean value2) throws Exception {
                        value1.setOrderTotalAmount(value1.getOrderTotalAmount().add(value2.getOrderTotalAmount()));
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderUuCount(value1.getOrderUuCount() + value2.getOrderUuCount());
                        return value1;
                    }
                }, new ProcessWindowFunction<DwsTradeSourceOrderWindowBean, DwsTradeSourceOrderWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsTradeSourceOrderWindowBean> elements, Collector<DwsTradeSourceOrderWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (DwsTradeSourceOrderWindowBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                });

        // TODO 11 维度关联来源名称
        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> sourceNameStream = AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<DwsTradeSourceOrderWindowBean>("dim_base_source".toUpperCase()) {
            @Override
            public void join(DwsTradeSourceOrderWindowBean bean, JSONObject jsonObject) throws Exception {
                bean.setSourceName(jsonObject.getString("source_site".toUpperCase()));
            }

            @Override
            public String getKey(DwsTradeSourceOrderWindowBean obj) {
                return obj.getSourceId();
            }
        }, 60 * 5, TimeUnit.SECONDS);

        // TODO 12 写出到clickHouse
        sourceNameStream.addSink(ClickHouseUtil.getJdbcSink("" +
                "insert into  dws_trade_source_order_window values(?,?,?,?,?,?,?,?)"));

        // TODO 13 执行任务
        env.execute();
    }
}
