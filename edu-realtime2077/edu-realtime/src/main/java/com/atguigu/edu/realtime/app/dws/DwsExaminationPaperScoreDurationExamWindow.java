package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsExaminationPaperScoreDurationExamWindowBean;
import com.atguigu.edu.realtime.bean.DwsInteractionCourseReviewWindowBean;
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
 * @create 2023-05-02 14:30
 */
public class DwsExaminationPaperScoreDurationExamWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境设置后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);
        // TODO 2 从kafka对应主题读取数据dwd_examination_test_paper
        String topicName = "dwd_examination_test_paper";
        String groupId = "dws_examination_paper_score_duration_exam_window";
        DataStreamSource<String> testPaperSource = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "test_paper_source");

        // TODO 3 转换结构添加度量值
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> beanStream = testPaperSource.flatMap(new FlatMapFunction<String, DwsExaminationPaperScoreDurationExamWindowBean>() {
            @Override
            public void flatMap(String value, Collector<DwsExaminationPaperScoreDurationExamWindowBean> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                Double score = jsonObject.getDouble("score");
                long ts = jsonObject.getLong("ts") * 1000;
                String scoreDuration;
                if (score < 60) {
                    scoreDuration = "[0,60)";
                } else if (score < 70) {
                    scoreDuration = "[60,70)";
                } else if (score < 80) {
                    scoreDuration = "[70,80)";
                } else if (score < 90) {
                    scoreDuration = "[80,90)";
                } else if (score <= 100) {
                    scoreDuration = "[90,100]";
                } else {
                    scoreDuration = "";
                }
                out.collect(DwsExaminationPaperScoreDurationExamWindowBean.builder()
                        .paper_id(jsonObject.getString("paper_id"))
                        .score_duration(scoreDuration)
                        .user_count(1L)
                        .ts(ts)
                        .build());
            }
        });

        // TODO 4 添加水位线
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsExaminationPaperScoreDurationExamWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsExaminationPaperScoreDurationExamWindowBean>() {
            @Override
            public long extractTimestamp(DwsExaminationPaperScoreDurationExamWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 按照试卷id和分区区间分组
        KeyedStream<DwsExaminationPaperScoreDurationExamWindowBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<DwsExaminationPaperScoreDurationExamWindowBean, String>() {
            @Override
            public String getKey(DwsExaminationPaperScoreDurationExamWindowBean value) throws Exception {
                return value.getPaper_id() + value.getScore_duration();
            }
        });

        // TODO 6 开窗聚合
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> reduceStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<DwsExaminationPaperScoreDurationExamWindowBean>() {
                    @Override
                    public DwsExaminationPaperScoreDurationExamWindowBean reduce(DwsExaminationPaperScoreDurationExamWindowBean value1, DwsExaminationPaperScoreDurationExamWindowBean value2) throws Exception {
                        value1.setUser_count(value1.getUser_count() + value2.getUser_count());
                        return value1;
                    }
                }, new ProcessWindowFunction<DwsExaminationPaperScoreDurationExamWindowBean, DwsExaminationPaperScoreDurationExamWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsExaminationPaperScoreDurationExamWindowBean> elements, Collector<DwsExaminationPaperScoreDurationExamWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (DwsExaminationPaperScoreDurationExamWindowBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                });

        // TODO 7 维度关联试卷名称
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> paperTitleStream = AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<DwsExaminationPaperScoreDurationExamWindowBean>("dim_test_paper".toUpperCase()) {
            @Override
            public void join(DwsExaminationPaperScoreDurationExamWindowBean bean, JSONObject jsonObject) throws Exception {
                bean.setPaper_title(jsonObject.getString("paper_title".toUpperCase()));
            }

            @Override
            public String getKey(DwsExaminationPaperScoreDurationExamWindowBean bean) {
                return bean.getPaper_id();
            }
        }, 60 * 5, TimeUnit.SECONDS);

        // TODO 8 写出到clickHouse
        paperTitleStream.addSink(ClickHouseUtil.getJdbcSink("" +
                "insert into dws_examination_paper_score_duration_exam_window values(?,?,?,?,?,?,?)"));

        // TODO 9 执行任务
        env.execute();
    }
}
