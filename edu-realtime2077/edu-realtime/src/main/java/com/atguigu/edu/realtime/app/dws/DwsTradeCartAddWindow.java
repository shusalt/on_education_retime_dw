package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwsTradeCartAddWindowBean;
import com.atguigu.edu.realtime.bean.DwsUserLoginWindowBean;
import com.atguigu.edu.realtime.util.ClickHouseUtil;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * @create 2023-05-01 20:05
 */
public class DwsTradeCartAddWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境设置后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        // TODO 2 从kafka加购主题中读取数据
        String topicName = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_window";
        DataStreamSource<String> cartAddSource = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "cart_add_source");

        // TODO 3 按照user_id进行分组
        KeyedStream<String, String> keyedStream = cartAddSource.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return jsonObject.getString("user_id");
            }
        });

        // TODO 4 使用状态过滤出独立用户
        SingleOutputStreamOperator<DwsTradeCartAddWindowBean> processStream = keyedStream.process(new KeyedProcessFunction<String, String, DwsTradeCartAddWindowBean>() {

            ValueState<String> lastCartAddState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastCartAddDtState = new ValueStateDescriptor<>("last_cart_add_dt_state", String.class);
                // 设置状态的过期时间
                lastCartAddDtState.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                lastCartAddState = getRuntimeContext().getState(lastCartAddDtState);
            }

            @Override
            public void processElement(String value, Context ctx, Collector<DwsTradeCartAddWindowBean> out) throws Exception {
                String lastCartAddDt = lastCartAddState.value();
                JSONObject jsonObject = JSON.parseObject(value);
                long ts = jsonObject.getLong("ts") * 1000;
                String curDate = DateFormatUtil.toDate(ts);
                if (lastCartAddDt == null || lastCartAddDt.compareTo(curDate) < 0) {
                    lastCartAddState.update(curDate);
                    DwsTradeCartAddWindowBean bean = DwsTradeCartAddWindowBean.builder()
                            .cartAddUvCount(1L)
                            .ts(ts)
                            .build();
                    out.collect(bean);
                }
            }
        });

        // TODO 5 设置水位线
        SingleOutputStreamOperator<DwsTradeCartAddWindowBean> withWaterMarkStream = processStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTradeCartAddWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeCartAddWindowBean>() {
            @Override
            public long extractTimestamp(DwsTradeCartAddWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 6 开窗聚合
        SingleOutputStreamOperator<DwsTradeCartAddWindowBean> reduceStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<DwsTradeCartAddWindowBean>() {
                    @Override
                    public DwsTradeCartAddWindowBean reduce(DwsTradeCartAddWindowBean value1, DwsTradeCartAddWindowBean value2) throws Exception {
                        value1.setCartAddUvCount(value1.getCartAddUvCount() + value2.getCartAddUvCount());
                        return value1;
                    }
                }, new ProcessAllWindowFunction<DwsTradeCartAddWindowBean, DwsTradeCartAddWindowBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsTradeCartAddWindowBean> elements, Collector<DwsTradeCartAddWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (DwsTradeCartAddWindowBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                });

        // TODO 7 写出到clickHouse
        reduceStream.addSink(ClickHouseUtil.getJdbcSink("" +
                "insert into dws_trade_cart_add_window values(?,?,?,?)"));
        // TODO 8 执行任务
        env.execute();

    }
}
