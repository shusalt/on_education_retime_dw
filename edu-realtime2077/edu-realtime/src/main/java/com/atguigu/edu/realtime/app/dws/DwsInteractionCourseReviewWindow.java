package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsInteractionCourseReviewWindowBean;
import com.atguigu.edu.realtime.bean.DwsTradeSourceOrderWindowBean;
import com.atguigu.edu.realtime.util.ClickHouseUtil;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author yhm
 * @create 2023-05-02 13:17
 */
public class DwsInteractionCourseReviewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境设置后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        // TODO 2 读取dwd对应主题dwd_interaction_review
        String topicName = "dwd_interaction_review";
        String groupId = "dws_interaction_course_review_window";
        DataStreamSource<String> interactionReviewSource = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "interaction_review_source");

        // TODO 3 转换结构补充度量值
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> beanStream = interactionReviewSource.flatMap(new FlatMapFunction<String, DwsInteractionCourseReviewWindowBean>() {
            @Override
            public void flatMap(String value, Collector<DwsInteractionCourseReviewWindowBean> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String courseId = jsonObject.getString("course_id");
                    Long reviewStars = jsonObject.getLong("review_stars");
                    long ts = jsonObject.getLong("ts") * 1000;
                    out.collect(DwsInteractionCourseReviewWindowBean.builder()
                            .courseId(courseId)
                            .reviewTotalStars(reviewStars)
                            .reviewUserCount(1L)
                            .goodReviewUserCount(reviewStars == 5L ? 1L : 0L)
                            .ts(ts)
                            .build());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // TODO 4 添加水位线
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsInteractionCourseReviewWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsInteractionCourseReviewWindowBean>() {
            @Override
            public long extractTimestamp(DwsInteractionCourseReviewWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 按照课程id分组
        KeyedStream<DwsInteractionCourseReviewWindowBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<DwsInteractionCourseReviewWindowBean, String>() {
            @Override
            public String getKey(DwsInteractionCourseReviewWindowBean value) throws Exception {
                return value.getCourseId();
            }
        });

        // TODO 6 开窗聚合
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> reduceStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<DwsInteractionCourseReviewWindowBean>() {
                    @Override
                    public DwsInteractionCourseReviewWindowBean reduce(DwsInteractionCourseReviewWindowBean value1, DwsInteractionCourseReviewWindowBean value2) throws Exception {
                        value1.setReviewUserCount(value1.getReviewUserCount() + value2.getReviewUserCount());
                        value1.setGoodReviewUserCount(value1.getGoodReviewUserCount() + value2.getGoodReviewUserCount());
                        value1.setReviewTotalStars(value1.getReviewTotalStars() + value2.getReviewTotalStars());
                        return value1;
                    }
                }, new ProcessWindowFunction<DwsInteractionCourseReviewWindowBean, DwsInteractionCourseReviewWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsInteractionCourseReviewWindowBean> elements, Collector<DwsInteractionCourseReviewWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (DwsInteractionCourseReviewWindowBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                });

        // TODO 7 维度关联课程信息
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> dimBeanStream = AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<DwsInteractionCourseReviewWindowBean>("dim_course_info".toUpperCase()) {
            @Override
            public void join(DwsInteractionCourseReviewWindowBean bean, JSONObject jsonObject) throws Exception {
                bean.setCourseName(jsonObject.getString("course_name".toUpperCase()));
            }

            @Override
            public String getKey(DwsInteractionCourseReviewWindowBean bean) {
                return bean.getCourseId();
            }
        }, 5 * 60, TimeUnit.SECONDS);

        // TODO 8 写出到clickHouse
        dimBeanStream.addSink(ClickHouseUtil.getJdbcSink("" +
                "insert into  dws_interaction_course_review_window values(?,?,?,?,?,?,?,?)"));

        // TODO 9 执行任务
        env.execute();
    }
}
