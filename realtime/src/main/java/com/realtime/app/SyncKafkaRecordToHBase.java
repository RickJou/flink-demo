package com.realtime.app;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.realtime.app.binlog.*;
import com.realtime.app.hbase.HBaseUtil;
import com.realtime.app.hbase.sink.HBasePutBinlogRecordsSink;
import com.realtime.app.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.util.List;
import java.util.Properties;

public class SyncKafkaRecordToHBase {

    private static Logger log = Logger.getLogger(SyncKafkaRecordToHBase.class);

    public static void main(String[] args) {
        try {
            //连接Hbase
            HBaseUtil.init();
            //初始化所有要同步的表
            CreateHBaseTableForTableSchema.create();

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            /*检查点相关,存储相关配置在flink.yml中进行了统一配置*/
            env.enableCheckpointing(10 * 1000); // 10秒保存一次检查点
            CheckpointConfig config = env.getCheckpointConfig();
            config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);//取消程序时保留检查点
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);//恰好一次语义


            //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);// 使用事件时间
            env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);// 使用提取时间,同步数据无需时间时间
            env.setParallelism(4);//并行度

            /*kafka属性*/
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "192.168.21.91:9090, 192.168.21.92:9090, 192.168.21.93:9090");
            properties.setProperty("group.id", "flink_default_group");
            properties.setProperty("auto.offset.reset", "earliest");
            properties.setProperty("enable.auto.commit", "false");
            properties.setProperty("group.max.session.timeout.ms", "300000");
            properties.setProperty("session.timeout.ms", "9000");
            properties.setProperty("heartbeat.interval.ms", "3000");

            FlinkKafkaConsumer<BinlogDmlPo> consumer = new FlinkKafkaConsumer<>(
                    //java.util.regex.Pattern.compile("(^t_ac_\\S*)|(^t_tc_\\S*)|(^t_uc_\\S*)"),//三个库中的表前缀匹配topic名称
                    java.util.regex.Pattern.compile("(^t_tc_project_wait_publish)"),
                    new BinlogDeserializationSchema(),//自定义反序列化器
                    properties);

            /*kafka record 读取模式*/
            consumer.setStartFromEarliest();     // 从最早记录开始读取

            /*增加时间和水位设置*/
            //consumer.assignTimestampsAndWatermarks(new KafkaJob().getBinlogAssignerWithPunctuatedWatermarks());

            DataStream<BinlogDmlPo> stream = env.addSource(consumer);
            //拆分一个record中包含多个sql
            DataStream<BinlogRecord> RecordStream = stream.flatMap(new FlatMapFunction<BinlogDmlPo, BinlogRecord>() {
                @Override
                public void flatMap(BinlogDmlPo binlogDmlPo, Collector<BinlogRecord> out) throws Exception {
                    for (int i = 0; i < binlogDmlPo.getRecords().size(); i++) {
                        JSONObject rec = binlogDmlPo.getRecords().getJSONObject(i);
                        BinlogRecord br = new BinlogRecord();
                        br.setTableName(binlogDmlPo.getTableName());
                        br.setDatabaseName(binlogDmlPo.getDatabaseName());
                        br.setRecord(rec);
                        br.setPrimaryKeyName(binlogDmlPo.getPrimaryKeyName());
                        out.collect(br);
                    }
                }
            });
            //.assignTimestampsAndWatermarks(new SyncKafkaRecordToHBase().getextractedTimestamp());//水位时间戳

            WindowedStream<BinlogRecord, String, TimeWindow> window =
                    //按同数据库,同表,同天,同一行记录进行分组
                    RecordStream.keyBy((KeySelector<BinlogRecord, String>) po -> po.getDatabaseName()+po.getTableName()+po.getRecord().getString(po.getPrimaryKeyName()))
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(60), Time.hours(0)))//处理时间翻滚窗口
                    .trigger(CountTrigger.of(1));
                    //.allowedLateness(Time.hours(1));//允许延时


            //微批数据去重(执行reduce取此批数据中,同id情况下update_time最大的一条)
            DataStream<BinlogRecord> dataStream =
                    window.reduce((ReduceFunction<BinlogRecord>) (t1, t2) -> {
                        String updateTimeKeyName = TableSchemaStore.getTableUpdateTimeKeyName(t1.getTableName());//可能是update_time;create_time
                        String t1UpdateTimeStr = t1.getRecord().getString(updateTimeKeyName);
                        String t2UpdateTimeStr = t2.getRecord().getString(updateTimeKeyName);
                        if(StringUtils.isNotBlank(t1UpdateTimeStr)&&StringUtils.isNotBlank(t2UpdateTimeStr)){
                            return  DateUtil.getTime(t1UpdateTimeStr)> DateUtil.getTime(t2UpdateTimeStr) ? t1 : t2;
                        }
                        return t2;
                    });


            //hbase结果写入,以1秒一次时间窗口为一批,汇总为list对象(sink输出时没有批量操作,此处相当于使得一批为一个元素)
            dataStream.timeWindowAll(Time.seconds(1)).apply(new AllWindowFunction<BinlogRecord, List<BinlogRecord>, TimeWindow>() {
                @Override
                public void apply(TimeWindow window, Iterable<BinlogRecord> values, Collector<List<BinlogRecord>> out) throws Exception {
                    out.collect(Lists.newArrayList(values));
                }
            }).addSink(new HBasePutBinlogRecordsSink());

            env.execute("Flink Streaming Java API Skeleton");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    /**
     * 以摄取时间作为水位时间
     * @return
     */
    public AssignerWithPunctuatedWatermarks getextractedTimestamp() {
        return new AssignerWithPunctuatedWatermarks<BinlogRecord>() {
            @Override
            public long extractTimestamp(BinlogRecord element, long previousElementTimestamp) {
                return previousElementTimestamp;
            }
            @Override
            public Watermark checkAndGetNextWatermark(BinlogRecord lastElement, long extractedTimestamp) {
                return new Watermark(extractedTimestamp);
            }
        };
    }

    /**
     * 以binlog中记录的update_time作为水位时间
     *
     * @return
     */
    public AssignerWithPunctuatedWatermarks getBinlogUpDateTimeAssignerWithPunctuatedWatermarks() {
        return new AssignerWithPunctuatedWatermarks<BinlogRecord>() {
            @Override
            public long extractTimestamp(BinlogRecord element, long previousElementTimestamp) {
                return getUpdateTime(element);
            }

            @Override
            public Watermark checkAndGetNextWatermark(BinlogRecord lastElement, long extractedTimestamp) {
                if (lastElement != null) {
                    return new Watermark(getUpdateTime(lastElement));
                }
                return null;
            }

            /**
             * binlog to kafka 时,一个消息中存在多个dml sql,取其中最大的update作为时间戳和watermark
             * @param br
             * @return
             */
            private Long getUpdateTime(BinlogRecord br) {
                String update_time = br.getRecord().getString("update_time");
                Long longtime = 0L;
                try {
                    longtime = DateUtil.getTime(update_time);
                } catch (ParseException e) {
                    log.error("无法转换解析后的update_time为long时间戳");
                    e.printStackTrace();
                }
                return longtime;
            }
        };
    }
}
