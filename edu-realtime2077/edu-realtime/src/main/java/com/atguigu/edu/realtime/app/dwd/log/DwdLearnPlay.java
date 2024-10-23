package com.atguigu.edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwdLearnPlayBean;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author yhm
 * @create 2023-04-23 14:21
 */
public class DwdLearnPlay {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建环境设置状态后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        //TODO 2 读取kafka播放日志数据
        String topicName = "dwd_traffic_play_pre_process";
        String groupId = "dwd_learn_play";
        DataStreamSource<String> playSource = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, groupId), WatermarkStrategy.noWatermarks(), "learn_play");

        //TODO 3 清洗转换
        SingleOutputStreamOperator<DwdLearnPlayBean> learnBeanStream = playSource.flatMap(new FlatMapFunction<String, DwdLearnPlayBean>() {
            @Override
            public void flatMap(String value, Collector<DwdLearnPlayBean> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    JSONObject common = jsonObject.getJSONObject("common");
                    JSONObject appVideo = jsonObject.getJSONObject("appVideo");
                    Long ts = jsonObject.getLong("ts");
                    DwdLearnPlayBean learnPlayBean = DwdLearnPlayBean.builder()
                            .provinceId(common.getString("ar"))
                            .brand(common.getString("ba"))
                            .channel(common.getString("ch"))
                            .isNew(common.getString("is_new"))
                            .model(common.getString("md"))
                            .machineId(common.getString("mid"))
                            .operatingSystem(common.getString("os"))
                            .sourceId(common.getString("sc"))
                            .sessionId(common.getString("sid"))
                            .userId(common.getString("uid"))
                            .versionCode(common.getString("vc"))
                            .playSec(appVideo.getInteger("play_sec"))
                            .videoId(appVideo.getString("video_id"))
                            .positionSec(appVideo.getInteger("position_sec"))
                            .ts(ts)
                            .build();
                    out.collect(learnPlayBean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        //TODO 4 添加水位线
        SingleOutputStreamOperator<DwdLearnPlayBean> withWatermarkStream = learnBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwdLearnPlayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
                new SerializableTimestampAssigner<DwdLearnPlayBean>() {
                    @Override
                    public long extractTimestamp(DwdLearnPlayBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }
        ));

        //TODO 5 按照会话id分组
        KeyedStream<DwdLearnPlayBean, String> keyedStream = withWatermarkStream.keyBy(new KeySelector<DwdLearnPlayBean, String>() {
            @Override
            public String getKey(DwdLearnPlayBean value) throws Exception {
                return value.getSessionId();
            }
        });

        //TODO 6 聚合统计
        WindowedStream<DwdLearnPlayBean, String, TimeWindow> windowStream = keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(3L)));
        SingleOutputStreamOperator<DwdLearnPlayBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwdLearnPlayBean>() {
                    @Override
                    public DwdLearnPlayBean reduce(DwdLearnPlayBean value1, DwdLearnPlayBean value2) throws Exception {
                        value1.setPlaySec(value1.getPlaySec() + value2.getPlaySec());
                        if (value2.getTs() > value1.getTs()) {
                            value1.setPositionSec(value2.getPositionSec());
                        }
                        return value1;
                    }
                }, new ProcessWindowFunction<DwdLearnPlayBean, DwdLearnPlayBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwdLearnPlayBean> elements, Collector<DwdLearnPlayBean> out) throws Exception {
                        for (DwdLearnPlayBean element : elements) {
                            out.collect(element);
                        }
                    }
                }
        );

        //TODO 7 转换结构
        SingleOutputStreamOperator<String> jsonStrStream = reducedStream.map(JSON::toJSONString);

        //TODO 8 输出到kafka主题Kafka dwd_learn_play
        String targetTopic = "dwd_learn_play";
        jsonStrStream.sinkTo(KafkaUtil.getKafkaProducer(targetTopic,"learn_pay_trans"));

        //TODO 9 执行任务
        env.execute();
    }
}
