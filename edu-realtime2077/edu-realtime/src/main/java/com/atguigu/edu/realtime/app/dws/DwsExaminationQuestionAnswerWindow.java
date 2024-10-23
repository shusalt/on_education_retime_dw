package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsExaminationPaperScoreDurationExamWindowBean;
import com.atguigu.edu.realtime.bean.DwsExaminationQuestionAnswerWindowBean;
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
 * @create 2023-05-02 14:50
 */
public class DwsExaminationQuestionAnswerWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境设置后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);
        // TODO 2 读取kafka答题主题的数据dwd_examination_test_question
        String topicName = "dwd_examination_test_question";
        String groupId = "dws_examination_question_answer_window";
        DataStreamSource<String> testQuestionSource = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "test_question_source");

        // TODO 3 转换结构补充度量值
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> beanStream = testQuestionSource.flatMap(new FlatMapFunction<String, DwsExaminationQuestionAnswerWindowBean>() {
            @Override
            public void flatMap(String value, Collector<DwsExaminationQuestionAnswerWindowBean> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String isCorrect = jsonObject.getString("is_correct");
                long ts = jsonObject.getLong("ts") * 1000;
                out.collect(DwsExaminationQuestionAnswerWindowBean.builder()
                        .question_id(jsonObject.getString("question_id"))
                        .answer_count(1L)
                        .correctAnswerCount("1".equals(isCorrect) ? 1L : 0L)
                        .ts(ts)
                        .build());
            }
        });

        // TODO 4 设置水位线
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsExaminationQuestionAnswerWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsExaminationQuestionAnswerWindowBean>() {
            @Override
            public long extractTimestamp(DwsExaminationQuestionAnswerWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 按照题目id分组
        KeyedStream<DwsExaminationQuestionAnswerWindowBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<DwsExaminationQuestionAnswerWindowBean, String>() {
            @Override
            public String getKey(DwsExaminationQuestionAnswerWindowBean value) throws Exception {
                return value.getQuestion_id();
            }
        });

        // TODO 6 开窗聚合
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> reduceStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<DwsExaminationQuestionAnswerWindowBean>() {
                    @Override
                    public DwsExaminationQuestionAnswerWindowBean reduce(DwsExaminationQuestionAnswerWindowBean value1, DwsExaminationQuestionAnswerWindowBean value2) throws Exception {
                        value1.setAnswer_count(value1.getAnswer_count() + value2.getAnswer_count());
                        value1.setCorrectAnswerCount(value1.getCorrectAnswerCount() + value2.getCorrectAnswerCount());
                        return value1;
                    }
                }, new ProcessWindowFunction<DwsExaminationQuestionAnswerWindowBean, DwsExaminationQuestionAnswerWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsExaminationQuestionAnswerWindowBean> elements, Collector<DwsExaminationQuestionAnswerWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (DwsExaminationQuestionAnswerWindowBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                });

        // TODO 7 维度关联题目信息
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> questionTxtBeanStream = AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<DwsExaminationQuestionAnswerWindowBean>("dim_test_question_info".toUpperCase()) {
            @Override
            public void join(DwsExaminationQuestionAnswerWindowBean bean, JSONObject jsonObject) throws Exception {
                bean.setQuestion_txt(jsonObject.getString("question_txt".toUpperCase()));
            }

            @Override
            public String getKey(DwsExaminationQuestionAnswerWindowBean bean) {
                return bean.getQuestion_id();
            }
        }, 5 * 60, TimeUnit.SECONDS);

        // TODO 8 写出到clickHouse
        questionTxtBeanStream.addSink(ClickHouseUtil.getJdbcSink("" +
                "insert into dws_examination_question_answer_window values(?,?,?,?,?,?,?)"));

        // TODO 9 执行任务
        env.execute();
    }
}
