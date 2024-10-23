package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsLearnChapterPlayWindowBean;
import com.atguigu.edu.realtime.bean.DwsTrafficPageViewWindowBean;
import com.atguigu.edu.realtime.util.ClickHouseUtil;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author yhm
 * @create 2023-05-01 7:39
 */
public class DwsLearnChapterPlayWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境设置后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        // TODO 2 从kafka主题播放日志读取数据dwd_traffic_play_pre_process
        String topicName = "dwd_learn_play";
        String groupId = "dws_learn_chapter_play_window";

        DataStreamSource<String> playSourceStream = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "play_source");

        // TODO 3 转换数据结构
        SingleOutputStreamOperator<DwsLearnChapterPlayWindowBean> beanStream = playSourceStream.map(new MapFunction<String, DwsLearnChapterPlayWindowBean>() {
            @Override
            public DwsLearnChapterPlayWindowBean map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                return DwsLearnChapterPlayWindowBean.builder()
                        .videoId(jsonObject.getString("videoId"))
                        .userId(jsonObject.getString("userId"))
                        .playTotalSec(jsonObject.getLong("playSec"))
                        .playCount(1L)
                        .ts(jsonObject.getLong("ts"))
                        .build();
            }
        });


        // TODO 4 根据用户id分组
        KeyedStream<DwsLearnChapterPlayWindowBean, String> keyedStream = beanStream.keyBy(new KeySelector<DwsLearnChapterPlayWindowBean, String>() {
            @Override
            public String getKey(DwsLearnChapterPlayWindowBean value) throws Exception {
                return value.getUserId();
            }
        });

        // TODO 5 过滤独立用户数
        SingleOutputStreamOperator<DwsLearnChapterPlayWindowBean> uuBeanStream = keyedStream.process(new KeyedProcessFunction<String, DwsLearnChapterPlayWindowBean, DwsLearnChapterPlayWindowBean>() {
            ValueState<String> lastPlayDtState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<String> lastPlayDtStateDesc = new ValueStateDescriptor<>("last_play_dt_state", String.class);
                lastPlayDtStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastPlayDtState = getRuntimeContext().getState(lastPlayDtStateDesc);
            }

            @Override
            public void processElement(DwsLearnChapterPlayWindowBean bean, Context ctx, Collector<DwsLearnChapterPlayWindowBean> out) throws Exception {
                String lastDt = lastPlayDtState.value();
                String curDt = DateFormatUtil.toDate(bean.getTs());
                if (lastDt == null || lastDt.compareTo(curDt) < 0) {
                    bean.setPlayUuCount(1L);
                    lastPlayDtState.update(curDt);
                } else {
                    bean.setPlayUuCount(0L);
                }
                out.collect(bean);
            }
        });

        // TODO 6 添加水位线
        SingleOutputStreamOperator<DwsLearnChapterPlayWindowBean> withWaterMarkStream = uuBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsLearnChapterPlayWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsLearnChapterPlayWindowBean>() {
            @Override
            public long extractTimestamp(DwsLearnChapterPlayWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 7 分组开窗聚合数据
        SingleOutputStreamOperator<DwsLearnChapterPlayWindowBean> reduceStream = withWaterMarkStream.keyBy(DwsLearnChapterPlayWindowBean::getVideoId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<DwsLearnChapterPlayWindowBean>() {
                    @Override
                    public DwsLearnChapterPlayWindowBean reduce(DwsLearnChapterPlayWindowBean value1, DwsLearnChapterPlayWindowBean value2) throws Exception {
                        // 相同组的数据度量值累加
                        value1.setPlayCount(value1.getPlayCount() + value2.getPlayCount());
                        value1.setPlayTotalSec(value1.getPlayTotalSec() + value2.getPlayTotalSec());
                        value1.setPlayUuCount(value1.getPlayUuCount() + value2.getPlayUuCount());
                        return value1;
                    }
                }, new ProcessWindowFunction<DwsLearnChapterPlayWindowBean, DwsLearnChapterPlayWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsLearnChapterPlayWindowBean> elements, Collector<DwsLearnChapterPlayWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (DwsLearnChapterPlayWindowBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                });
        // TODO 8 补全维度信息
        // 获取章节id
        SingleOutputStreamOperator<DwsLearnChapterPlayWindowBean> chapterIdBeanStream = AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<DwsLearnChapterPlayWindowBean>("dim_video_info".toUpperCase()) {
            @Override
            public void join(DwsLearnChapterPlayWindowBean bean, JSONObject jsonObject) throws Exception {
                bean.setChapterId(jsonObject.getString("chapter_id".toUpperCase()));
            }

            @Override
            public String getKey(DwsLearnChapterPlayWindowBean obj) {
                return obj.getVideoId();
            }
        }, 60 * 5, TimeUnit.SECONDS);

        // 获取章节名称
        SingleOutputStreamOperator<DwsLearnChapterPlayWindowBean> dimBeanStream = AsyncDataStream.unorderedWait(chapterIdBeanStream, new DimAsyncFunction<DwsLearnChapterPlayWindowBean>("dim_chapter_info".toUpperCase()) {
            @Override
            public void join(DwsLearnChapterPlayWindowBean bean, JSONObject jsonObject) throws Exception {
                bean.setChapterName(jsonObject.getString("chapter_name".toUpperCase()));
            }

            @Override
            public String getKey(DwsLearnChapterPlayWindowBean obj) {
                return obj.getChapterId();
            }
        }, 60 * 5, TimeUnit.SECONDS);


        // TODO 9 写出到clickHouse
        dimBeanStream.addSink(ClickHouseUtil.getJdbcSink("" +
                "insert into dws_learn_chapter_play_window values(?,?,?,?,?,?,?,?)"));

        // TODO 10 执行任务
        env.execute();
    }
}
