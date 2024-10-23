package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsTradeProvinceOrderWindowBean;
import com.atguigu.edu.realtime.bean.DwsTradeSourceOrderWindowBean;
import com.atguigu.edu.realtime.util.ClickHouseUtil;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
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
 * @create 2023-05-02 11:32
 */
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境设置后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        // TODO 2 读取topic_db主题数据
        String topicName = "topic_db";
        String groupId = "dws_trade_province_order_window";
        DataStreamSource<String> topicDbSource = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "topic_db_source");

        // TODO 3 筛选订单表格数据 同时转换数据结构
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> orderInfoBeanStream = topicDbSource.flatMap(new FlatMapFunction<String, DwsTradeProvinceOrderWindowBean>() {
            @Override
            public void flatMap(String value, Collector<DwsTradeProvinceOrderWindowBean> out) throws Exception {
                try {
                    // maxwell生成的json字符串
                    JSONObject jsonObject = JSON.parseObject(value);
                    String table = jsonObject.getString("table");
                    String type = jsonObject.getString("type");
                    if ("order_info".equals(table) && "insert".equals(type)) {
                        JSONObject data = jsonObject.getJSONObject("data");
                        out.collect(DwsTradeProvinceOrderWindowBean.builder()
                                .orderCount(1L)
                                .orderTotalAmount(new BigDecimal(data.getString("final_amount")))
                                .userId(data.getString("user_id"))
                                .provinceId(data.getString("province_id"))
                                .ts(jsonObject.getLong("ts") * 1000)
                                .build());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // TODO 4 按照province_id和user_id分组
        KeyedStream<DwsTradeProvinceOrderWindowBean, String> provinceIdUserIdStream = orderInfoBeanStream.keyBy(new KeySelector<DwsTradeProvinceOrderWindowBean, String>() {
            @Override
            public String getKey(DwsTradeProvinceOrderWindowBean value) throws Exception {
                return value.getProvinceId() + value.getUserId();
            }
        });

        // TODO 5 统计独立用户
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> uuCountStream = provinceIdUserIdStream.process(new KeyedProcessFunction<String, DwsTradeProvinceOrderWindowBean, DwsTradeProvinceOrderWindowBean>() {

            ValueState<String> lastOrderDtState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastOrderDtStateDesc = new ValueStateDescriptor<>("last_order_dt_state", String.class);
                lastOrderDtStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());

                lastOrderDtState = getRuntimeContext().getState(lastOrderDtStateDesc);
            }

            @Override
            public void processElement(DwsTradeProvinceOrderWindowBean bean, Context ctx, Collector<DwsTradeProvinceOrderWindowBean> out) throws Exception {
                String lastDt = lastOrderDtState.value();
                Long ts = bean.getTs();
                String curDt = DateFormatUtil.toDate(ts);
                Long orderUuCount = 0L;
                if (lastDt == null || lastDt.compareTo(curDt) < 0) {
                    orderUuCount = 1L;
                    lastOrderDtState.update(curDt);
                }

                bean.setOrderUuCount(orderUuCount);
                out.collect(bean);
            }
        });

        // TODO 6 设置水位线
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> withWaterMarkStream = uuCountStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTradeProvinceOrderWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeProvinceOrderWindowBean>() {
            @Override
            public long extractTimestamp(DwsTradeProvinceOrderWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 7 按照province_id分组
        KeyedStream<DwsTradeProvinceOrderWindowBean, String> provinceIdStream = withWaterMarkStream.keyBy(new KeySelector<DwsTradeProvinceOrderWindowBean, String>() {
            @Override
            public String getKey(DwsTradeProvinceOrderWindowBean value) throws Exception {
                return value.getProvinceId();
            }
        });

        // TODO 8 开窗聚合
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> reduceStream = provinceIdStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<DwsTradeProvinceOrderWindowBean>() {
                    @Override
                    public DwsTradeProvinceOrderWindowBean reduce(DwsTradeProvinceOrderWindowBean value1, DwsTradeProvinceOrderWindowBean value2) throws Exception {
                        value1.setOrderUuCount(value1.getOrderUuCount() + value2.getOrderUuCount());
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderTotalAmount(value1.getOrderTotalAmount().add(value2.getOrderTotalAmount()));
                        return value1;
                    }
                }, new ProcessWindowFunction<DwsTradeProvinceOrderWindowBean, DwsTradeProvinceOrderWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsTradeProvinceOrderWindowBean> elements, Collector<DwsTradeProvinceOrderWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (DwsTradeProvinceOrderWindowBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                });

        // TODO 9 维度关联province_name
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> provinceNameStream = AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<DwsTradeProvinceOrderWindowBean>("dim_base_province".toUpperCase()) {
            @Override
            public void join(DwsTradeProvinceOrderWindowBean obj, JSONObject jsonObject) throws Exception {
                obj.setProvinceName(jsonObject.getString("name".toUpperCase()));
            }

            @Override
            public String getKey(DwsTradeProvinceOrderWindowBean obj) {
                return obj.getProvinceId();
            }
        }, 60 * 5, TimeUnit.SECONDS);

        // TODO 10 写出到clickHouse
        provinceNameStream.addSink(ClickHouseUtil.getJdbcSink("" +
                "insert into dws_trade_province_order_window values(?,?,?,?,?,?,?,?)"));

        // TODO 11 执行任务
        env.execute();
    }
}
