package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwsTrafficPageViewWindowBean;
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
 * @create 2023-04-30 21:06
 */
public class DwsTrafficPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境设置后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        // TODO 2 从page_log读取数据
        String topicName = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window";
        DataStreamSource<String> logStream = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "log_stream");

        // TODO 3 对数据进行过滤转换
        SingleOutputStreamOperator<DwsTrafficPageViewWindowBean> beanStream = logStream.flatMap(new FlatMapFunction<String, DwsTrafficPageViewWindowBean>() {
            @Override
            public void flatMap(String value, Collector<DwsTrafficPageViewWindowBean> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject page = jsonObject.getJSONObject("page");
                JSONObject common = jsonObject.getJSONObject("common");
                String pageId = page.getString("page_id");
                if ("home".equals(pageId) || "course_list".equals(pageId) || "course_detail".equals(pageId)) {
                    out.collect(DwsTrafficPageViewWindowBean.builder()
                            .mid(common.getString("mid"))
                            .pageId(pageId)
                            .ts(jsonObject.getLong("ts"))
                            .build());
                }
            }
        });

        // TODO 4 按照mid进行分组
        KeyedStream<DwsTrafficPageViewWindowBean, String> keyedStream = beanStream.keyBy((KeySelector<DwsTrafficPageViewWindowBean, String>) DwsTrafficPageViewWindowBean::getMid);

        // TODO 5 使用flink状态判断当前数据是否为独立访客
        SingleOutputStreamOperator<DwsTrafficPageViewWindowBean> uvBeanStream = keyedStream.process(new KeyedProcessFunction<String, DwsTrafficPageViewWindowBean, DwsTrafficPageViewWindowBean>() {
            ValueState<String> homeState = null;
            ValueState<String> listState = null;
            ValueState<String> detailState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<String> homeLastDtState = new ValueStateDescriptor<>("home_last_dt_state", String.class);
                // 设置过期时间
                homeLastDtState.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                ValueStateDescriptor<String> courseListLastDtState = new ValueStateDescriptor<>("course_list_last_dt_state", String.class);
                courseListLastDtState.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                ValueStateDescriptor<String> courseDetailLastDtState = new ValueStateDescriptor<>("course_detail_last_dt_state", String.class);
                courseDetailLastDtState.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());

                homeState = getRuntimeContext().getState(homeLastDtState);
                listState = getRuntimeContext().getState(courseListLastDtState);
                detailState = getRuntimeContext().getState(courseDetailLastDtState);

            }

            @Override
            public void processElement(DwsTrafficPageViewWindowBean bean, Context ctx, Collector<DwsTrafficPageViewWindowBean> out) throws Exception {
                String pageId = bean.getPageId();
                String curDate = DateFormatUtil.toDate(bean.getTs());
                // 首先将度量值先置为0
                bean.setHomeUvCount(0L);
                bean.setListUvCount(0L);
                bean.setDetailUvCount(0L);
                switch (pageId) {
                    case "home":
                        String homeLastDt = homeState.value();
                        if (homeLastDt == null || homeLastDt.compareTo(curDate) < 0) {
                            // 判断为独立访客
                            bean.setHomeUvCount(1L);
                            homeState.update(curDate);
                            out.collect(bean);
                        }
                        break;
                    case "course_list":
                        String listLastDt = listState.value();
                        if (listLastDt == null || listLastDt.compareTo(curDate) < 0) {
                            // 判断为独立访客
                            bean.setListUvCount(1L);
                            listState.update(curDate);
                            out.collect(bean);
                        }
                        break;
                    case "course_detail":
                        String detailLastDt = detailState.value();
                        if (detailLastDt == null || detailLastDt.compareTo(curDate) < 0) {
                            // 判断为独立访客
                            bean.setDetailUvCount(1L);
                            detailState.update(curDate);
                            out.collect(bean);
                        }
                        break;

                }

            }
        });

        // TODO 6 设置水位线
        SingleOutputStreamOperator<DwsTrafficPageViewWindowBean> withWaterMarkStream = uvBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTrafficPageViewWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsTrafficPageViewWindowBean>() {
            @Override
            public long extractTimestamp(DwsTrafficPageViewWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 7 开窗聚合
        SingleOutputStreamOperator<DwsTrafficPageViewWindowBean> reduceStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<DwsTrafficPageViewWindowBean>() {
                            @Override
                            public DwsTrafficPageViewWindowBean reduce(DwsTrafficPageViewWindowBean value1, DwsTrafficPageViewWindowBean value2) throws Exception {
                                value1.setHomeUvCount(value1.getHomeUvCount() + value2.getHomeUvCount());
                                value1.setListUvCount(value1.getListUvCount() + value2.getListUvCount());
                                value1.setDetailUvCount(value1.getDetailUvCount() + value2.getDetailUvCount());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<DwsTrafficPageViewWindowBean, DwsTrafficPageViewWindowBean, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<DwsTrafficPageViewWindowBean> elements, Collector<DwsTrafficPageViewWindowBean> out) throws Exception {
                                // 添加窗口的开始时间和关闭时间
                                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                                for (DwsTrafficPageViewWindowBean element : elements) {
                                    element.setStt(stt);
                                    element.setEdt(edt);
                                    element.setTs(System.currentTimeMillis());
                                    out.collect(element);
                                }

                            }
                        });

        // TODO 8 写出到clickHouse
        reduceStream.addSink(ClickHouseUtil.getJdbcSink("" +
                "insert into dws_traffic_page_view_window values(?,?,?,?,?,?)"));

        // TODO 9 执行任务
        env.execute();
    }
}
