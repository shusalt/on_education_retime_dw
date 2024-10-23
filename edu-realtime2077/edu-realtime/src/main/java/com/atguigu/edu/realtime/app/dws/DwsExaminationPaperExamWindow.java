package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsExaminationPaperExamWindowBean;
import com.atguigu.edu.realtime.bean.DwsTradeProvinceOrderWindowBean;
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
 * @create 2023-05-02 13:40
 */
public class DwsExaminationPaperExamWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境和设置后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        // TODO 2 读取对应主题数据dwd_examination_test_paper
        String topicName = "dwd_examination_test_paper";
        String groupId = "dws_examination_paper_exam_window";
        DataStreamSource<String> testPaperSource = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "test_paper_source");

        // TODO 3 转换结构添加度量值
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> beanStream = testPaperSource.flatMap(new FlatMapFunction<String, DwsExaminationPaperExamWindowBean>() {
            @Override
            public void flatMap(String value, Collector<DwsExaminationPaperExamWindowBean> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                long ts = jsonObject.getLong("ts") * 1000;
                Double score = jsonObject.getDouble("score");

                out.collect(DwsExaminationPaperExamWindowBean.builder()
                        .paperId(jsonObject.getString("paper_id"))
                        .examTakenCount(1L)
                        .examTotalScore((long) score.doubleValue())
                        .examTotalDuringSec(jsonObject.getLong("duration_sec"))
                        .ts(ts)
                        .build());
            }
        });

        // TODO 4 添加水位线
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsExaminationPaperExamWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsExaminationPaperExamWindowBean>() {
            @Override
            public long extractTimestamp(DwsExaminationPaperExamWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 分组开窗聚合
        KeyedStream<DwsExaminationPaperExamWindowBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<DwsExaminationPaperExamWindowBean, String>() {
            @Override
            public String getKey(DwsExaminationPaperExamWindowBean value) throws Exception {
                return value.getPaperId();
            }
        });

        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> reduceStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<DwsExaminationPaperExamWindowBean>() {
                    @Override
                    public DwsExaminationPaperExamWindowBean reduce(DwsExaminationPaperExamWindowBean value1, DwsExaminationPaperExamWindowBean value2) throws Exception {
                        value1.setExamTakenCount(value1.getExamTakenCount() + value2.getExamTakenCount());
                        value1.setExamTotalDuringSec(value1.getExamTotalDuringSec() + value2.getExamTotalDuringSec());
                        value1.setExamTotalScore(value1.getExamTotalScore() + value2.getExamTotalScore());
                        return value1;
                    }
                }, new ProcessWindowFunction<DwsExaminationPaperExamWindowBean, DwsExaminationPaperExamWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsExaminationPaperExamWindowBean> elements, Collector<DwsExaminationPaperExamWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (DwsExaminationPaperExamWindowBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                });

        // TODO 6 维度关联补充维度信息
        // 维度关联试卷表
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> paperStream = AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<DwsExaminationPaperExamWindowBean>("dim_test_paper".toUpperCase()) {
            @Override
            public void join(DwsExaminationPaperExamWindowBean bean, JSONObject jsonObject) throws Exception {
                bean.setPaperTitle(jsonObject.getString("paper_title".toUpperCase()));
                bean.setCourseId(jsonObject.getString("course_id".toUpperCase()));
            }

            @Override
            public String getKey(DwsExaminationPaperExamWindowBean bean) {
                return bean.getPaperId();
            }
        }, 5 * 60, TimeUnit.SECONDS);

        // 维度关联课程表
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> courseBeanStream = AsyncDataStream.unorderedWait(paperStream, new DimAsyncFunction<DwsExaminationPaperExamWindowBean>("dim_course_info".toUpperCase()) {
            @Override
            public void join(DwsExaminationPaperExamWindowBean bean, JSONObject jsonObject) throws Exception {

                bean.setCourseName(jsonObject.getString("course_name".toUpperCase()));
            }

            @Override
            public String getKey(DwsExaminationPaperExamWindowBean bean) {
                return bean.getCourseId();
            }
        }, 5 * 60, TimeUnit.SECONDS);


        // TODO 7 写出到clickHouse
        courseBeanStream.addSink(ClickHouseUtil.getJdbcSink("" +
                "insert into dws_examination_paper_exam_window values(?,?,?,?,?,?,?,?,?,?)"));

        // TODO 8 执行任务
        env.execute();
    }
}
