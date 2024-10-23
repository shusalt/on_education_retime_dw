package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsTradeCourseOrderWindowBean;
import com.atguigu.edu.realtime.bean.DwsTradeOrderWindowBean;
import com.atguigu.edu.realtime.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
 * @create 2023-05-02 9:40
 */
public class DwsTradeCourseOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1  创建环境设置后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        // TODO 2 读取dwd层订单详情主题数据dwd_trade_order_detail
        String topicName = "dwd_trade_order_detail";
        String groupId = "dws_trade_course_order_window";
        DataStreamSource<String> orderDetailSource = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "order_detail_source");

        // TODO 3 过滤null值转换数据结构
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

        // TODO 4 按照订单明细ID进行分组
        // 找到回撤流完整的数据
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("id");
            }
        });

        // TODO 5 使用状态去重回撤流数据
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

        // TODO 6 转换结构为javaBean
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> beanStream = processStream.map(new MapFunction<JSONObject, DwsTradeCourseOrderWindowBean>() {
            @Override
            public DwsTradeCourseOrderWindowBean map(JSONObject jsonObj) throws Exception {

                return DwsTradeCourseOrderWindowBean.builder()
                        .courseId(jsonObj.getString("course_id"))
                        .orderTotalAmount(new BigDecimal(jsonObj.getString("final_amount")))
                        .ts(jsonObj.getLong("ts") * 1000)
                        .build();
            }
        });

        // TODO 7 添加水位线
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTradeCourseOrderWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeCourseOrderWindowBean>() {
            @Override
            public long extractTimestamp(DwsTradeCourseOrderWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 8 按照课程ID进行分组
        KeyedStream<DwsTradeCourseOrderWindowBean, String> courseIdStream = withWaterMarkStream.keyBy(new KeySelector<DwsTradeCourseOrderWindowBean, String>() {
            @Override
            public String getKey(DwsTradeCourseOrderWindowBean value) throws Exception {
                return value.getCourseId();
            }
        });

        // TODO 9 开窗聚合
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> reduceStream = courseIdStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<DwsTradeCourseOrderWindowBean>() {
                    @Override
                    public DwsTradeCourseOrderWindowBean reduce(DwsTradeCourseOrderWindowBean value1, DwsTradeCourseOrderWindowBean value2) throws Exception {
                        value1.setOrderTotalAmount(value1.getOrderTotalAmount().add(value2.getOrderTotalAmount()));
                        return value1;
                    }
                }, new ProcessWindowFunction<DwsTradeCourseOrderWindowBean, DwsTradeCourseOrderWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsTradeCourseOrderWindowBean> elements, Collector<DwsTradeCourseOrderWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (DwsTradeCourseOrderWindowBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                });

        // TODO 10 维度关联
        // 关联课程名称以及科目id
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> courseNameBeanStream = AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<DwsTradeCourseOrderWindowBean>("dim_course_info".toUpperCase()) {
            @Override
            public void join(DwsTradeCourseOrderWindowBean bean, JSONObject jsonObject) throws Exception {
                bean.setCourseName(jsonObject.getString("course_name".toUpperCase()));
                bean.setSubjectId(jsonObject.getString("subject_id".toUpperCase()));
            }

            @Override
            public String getKey(DwsTradeCourseOrderWindowBean obj) {
                return obj.getCourseId();
            }
        }, 60 * 5, TimeUnit.SECONDS);

        // 关联科目表
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> subjectNameBeanStream = AsyncDataStream.unorderedWait(courseNameBeanStream, new DimAsyncFunction<DwsTradeCourseOrderWindowBean>("dim_base_subject_info".toUpperCase()) {
            @Override
            public void join(DwsTradeCourseOrderWindowBean bean, JSONObject jsonObject) throws Exception {
                bean.setSubjectName(jsonObject.getString("subject_name".toUpperCase()));
                bean.setCategoryId(jsonObject.getString("category_id".toUpperCase()));
            }

            @Override
            public String getKey(DwsTradeCourseOrderWindowBean obj) {
                return obj.getSubjectId();
            }
        }, 60 * 5, TimeUnit.SECONDS);

        // 关联类别表
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> categoryNameBeanStream = AsyncDataStream.unorderedWait(subjectNameBeanStream, new DimAsyncFunction<DwsTradeCourseOrderWindowBean>("dim_base_category_info".toUpperCase()) {
            @Override
            public void join(DwsTradeCourseOrderWindowBean bean, JSONObject jsonObject) throws Exception {

                bean.setCategoryName(jsonObject.getString("category_name".toUpperCase()));
            }

            @Override
            public String getKey(DwsTradeCourseOrderWindowBean obj) {
                return obj.getCategoryId();
            }
        }, 60 * 5, TimeUnit.SECONDS);


        // TODO 11 写出到clickHouse
        categoryNameBeanStream.addSink(ClickHouseUtil.getJdbcSink("" +
                "insert into dws_trade_course_order_window values(?,?,?,?,?,?,?,?,?,?)"));

        // TODO 12 执行任务
        env.execute();
    }
}
