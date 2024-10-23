package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsTrafficForSourcePvBean;
import com.atguigu.edu.realtime.util.ClickHouseUtil;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.*;
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
 * @create 2023-04-26 16:08
 */
public class DwsTrafficVcSourceArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建环境设置状态后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        //TODO 2 读取pageLog主题数据
        String pageTopic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_vc_source_ar_is_new_page_view_window";
        KafkaSource<String> pageSource= KafkaUtil.getKafkaConsumer(pageTopic, groupId);
        DataStreamSource<String> pageStream = env.fromSource(pageSource, WatermarkStrategy.noWatermarks(),"page_log");

        //TODO 3 读取独立访客数据
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        KafkaSource<String> uvSource= KafkaUtil.getKafkaConsumer(uvTopic, groupId);
        DataStreamSource<String> uvStream = env.fromSource(uvSource, WatermarkStrategy.noWatermarks(),"uv_detail");

        //TODO 4 读取跳出用户数据
        String jumpTopic = "dwd_traffic_user_jump_detail";
        KafkaSource<String> jumpSource= KafkaUtil.getKafkaConsumer(jumpTopic, groupId);
        DataStreamSource<String> jumpStream = env.fromSource(jumpSource, WatermarkStrategy.noWatermarks(),"jump_detail");


        //TODO 5 转换数据结构
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> pageBeanStream = pageStream.map(new MapFunction<String, DwsTrafficForSourcePvBean>() {
            @Override
            public DwsTrafficForSourcePvBean map(String value) throws Exception {
                // 将page_log的一条日志转换为一个对应的javaBean
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                Long ts = jsonObject.getLong("ts");

                return DwsTrafficForSourcePvBean.builder()
                        .versionCode(common.getString("vc"))
                        .sourceId(common.getString("sc"))
                        .ar(common.getString("ar"))
                        .isNew(common.getString("is_new"))
                        .uvCount(0L)
                        .totalSessionCount(page.getString("last_page_id") == null ? 1L : 0L)
                        .pageViewCount(1L)
                        .totalDuringTime(page.getLong("during_time"))
                        .jumpSessionCount(0L)
                        .ts(ts)
                        .build();
            }
        });

        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> uvBeanStream = uvStream.map(new MapFunction<String, DwsTrafficForSourcePvBean>() {
            @Override
            public DwsTrafficForSourcePvBean map(String value) throws Exception {
                // 将page_log的一条日志转换为一个对应的javaBean
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");

                return DwsTrafficForSourcePvBean.builder()
                        .versionCode(common.getString("vc"))
                        .sourceId(common.getString("sc"))
                        .ar(common.getString("ar"))
                        .isNew(common.getString("is_new"))
                        .uvCount(1L)
                        .totalSessionCount(0L)
                        .pageViewCount(0L)
                        .totalDuringTime(0L)
                        .jumpSessionCount(0L)
                        .ts(ts)
                        .build();
            }
        });

        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> jumpBeanStream = jumpStream.map(new MapFunction<String, DwsTrafficForSourcePvBean>() {
            @Override
            public DwsTrafficForSourcePvBean map(String value) throws Exception {
                // 将page_log的一条日志转换为一个对应的javaBean
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");

                return DwsTrafficForSourcePvBean.builder()
                        .versionCode(common.getString("vc"))
                        .sourceId(common.getString("sc"))
                        .ar(common.getString("ar"))
                        .isNew(common.getString("is_new"))
                        .uvCount(0L)
                        .totalSessionCount(0L)
                        .pageViewCount(0L)
                        .totalDuringTime(0L)
                        .jumpSessionCount(1L)
                        .ts(ts)
                        .build();
            }
        });


        //TODO 6 合并3条数据流
        DataStream<DwsTrafficForSourcePvBean> unionStream = pageBeanStream.union(uvBeanStream).union(jumpBeanStream);



        //TODO 7 添加水位线
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> withWaterMarkStream = unionStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTrafficForSourcePvBean>forBoundedOutOfOrderness(Duration.ofSeconds(15L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsTrafficForSourcePvBean>() {
            @Override
            public long extractTimestamp(DwsTrafficForSourcePvBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 8 分组开窗
        WindowedStream<DwsTrafficForSourcePvBean, String, TimeWindow> windowStream = withWaterMarkStream.keyBy(new KeySelector<DwsTrafficForSourcePvBean, String>() {
            @Override
            public String getKey(DwsTrafficForSourcePvBean value) throws Exception {
                return value.getVersionCode()
                        + value.getSourceId()
                        + value.getAr()
                        + value.getIsNew();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10L)));


        //TODO 9 聚合统计
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsTrafficForSourcePvBean>() {
            @Override
            public DwsTrafficForSourcePvBean reduce(DwsTrafficForSourcePvBean value1, DwsTrafficForSourcePvBean value2) throws Exception {
                // 合并相同common信息的数据
                value1.setTotalSessionCount(value1.getTotalSessionCount() + value2.getTotalSessionCount());
                value1.setUvCount(value1.getUvCount() + value2.getUvCount());
                value1.setTotalDuringTime(value1.getTotalDuringTime() + value2.getTotalDuringTime());
                value1.setJumpSessionCount(value1.getJumpSessionCount() + value2.getJumpSessionCount());
                value1.setPageViewCount(value1.getPageViewCount() + value2.getPageViewCount());
                return value1;
            }
        }, new ProcessWindowFunction<DwsTrafficForSourcePvBean, DwsTrafficForSourcePvBean, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<DwsTrafficForSourcePvBean> elements, Collector<DwsTrafficForSourcePvBean> out) throws Exception {
                TimeWindow timeWindow = context.window();
                String start = DateFormatUtil.toYmdHms(timeWindow.getStart());
                String end = DateFormatUtil.toYmdHms(timeWindow.getEnd());
                for (DwsTrafficForSourcePvBean element : elements) {
                    element.setStt(start);
                    element.setEdt(end);
                    // 修正时间戳
                    element.setTs(System.currentTimeMillis());
                    out.collect(element);
                }
            }
        });
//        reduceStream.print();

        //TODO 10 维度关联
//        reduceStream.map(new MapFunction<DwsTrafficForSourcePvBean, DwsTrafficForSourcePvBean>() {
//            @Override
//            public DwsTrafficForSourcePvBean map(DwsTrafficForSourcePvBean value) throws Exception {
//                // 关联来源名称
//                String sourceId = value.getSourceId();
//                String provinceId = value.getAr();
//                JSONObject dimBaseSource = DimUtil.getDimInfo("DIM_BASE_SOURCE", sourceId);
//                String sourceName = dimBaseSource.getString("SOURCE_SITE");
//                value.setSourceName(sourceName);
//                JSONObject dimBaseProvince = DimUtil.getDimInfo("DIM_BASE_PROVINCE",provinceId);
//                String provinceName = dimBaseProvince.getString("NAME");
//                value.setProvinceName(provinceName);
//                return value;
//            }
//        }).print();

        // 异步操作
        // 关联来源表
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> sourceBeanStream = AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<DwsTrafficForSourcePvBean>("DIM_BASE_SOURCE") {
            @Override
            public void join(DwsTrafficForSourcePvBean obj, JSONObject jsonObject) throws Exception {
                String sourceName = jsonObject.getString("SOURCE_SITE");
                obj.setSourceName(sourceName);
            }

            @Override
            public String getKey(DwsTrafficForSourcePvBean obj) {
                return obj.getSourceId();
            }
        }, 1, TimeUnit.MINUTES);

        // 关联省份
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> dimBeanStream = AsyncDataStream.unorderedWait(sourceBeanStream, new DimAsyncFunction<DwsTrafficForSourcePvBean>("DIM_BASE_PROVINCE") {
            @Override
            public void join(DwsTrafficForSourcePvBean obj, JSONObject jsonObject) throws Exception {
                String provinceName = jsonObject.getString("NAME");
                obj.setProvinceName(provinceName);
            }

            @Override
            public String getKey(DwsTrafficForSourcePvBean obj) {
                return obj.getAr();
            }
        }, 1, TimeUnit.MINUTES);



        //TODO 11 写出到clickHouse
        dimBeanStream.addSink(ClickHouseUtil.getJdbcSink(" " +
                "insert into dws_traffic_vc_source_ar_is_new_page_view_window values" +
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        // TODO 12 执行任务
        env.execute();


    }
}
