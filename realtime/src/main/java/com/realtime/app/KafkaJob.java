package com.realtime.app;

import com.alibaba.fastjson.JSONArray;
import com.realtime.app.deserializer.BinlogDeserializationSchema;
import com.realtime.app.deserializer.BinlogDmlPo;
import com.realtime.app.kafka.convert.BinlogConvert;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.util.Properties;

public class KafkaJob {

    private static Logger log = Logger.getLogger(KafkaJob.class);

    public static void main(String[] args) {
        try {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            /*检查点相关,存储相关配置在flink.yml中进行了统一配置*/
            env.enableCheckpointing(10*1000); // 10秒保存一次检查点
            CheckpointConfig config = env.getCheckpointConfig();
            config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);//取消程序时保留检查点
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);//恰好一次语义


            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);// 使用事件时间


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
                    //java.util.regex.Pattern.compile("^(t_ac_*)|(t_tc_*)|(t_uc_*)"),//三个库中的表前缀匹配topic名称
                    java.util.regex.Pattern.compile("(^t_ac_\\S*)|(^t_tc_\\S*)|(^t_uc_\\S*)"),
                    new BinlogDeserializationSchema(),//自定义反序列化器
                    properties);


            /*kafka record 读取模式*/
            consumer.setStartFromEarliest();     // 从最早记录开始读取

            //consumer.setStartFromLatest();       // 从最新记录开始读取
            //consumer.setStartFromTimestamp(...); // 从指定时间戳(毫秒)开始
            //consumer.setStartFromGroupOffsets(); // 默认使用此项,读取保存在kafka队列中group 的分区的偏移量 (会丢数据,kafka自行维护的offset不可靠)
            /*Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
            specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 0L);
            specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 10L);
            specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 20L);
            consumer.setStartFromSpecificOffsets(specificStartOffsets);//从消费组指定offset位置开始读取record ,the default behaviour
            */


            /*增加时间和水位设置*/
            consumer.assignTimestampsAndWatermarks(new KafkaJob().getBinlogAssignerWithPunctuatedWatermarks());


            DataStream<BinlogDmlPo> stream = env.addSource(consumer);

            //将源数据保存到hbase,将create_time和update_time索引数据保存到redis



            stream.print();

            env.execute("Flink Streaming Java API Skeleton");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public AssignerWithPunctuatedWatermarks getBinlogAssignerWithPunctuatedWatermarks() {
        return new AssignerWithPunctuatedWatermarks<BinlogDmlPo>() {
            @Override
            public long extractTimestamp(BinlogDmlPo element, long previousElementTimestamp) {
                return getUpdateTime(element);
            }


            @Override
            public Watermark checkAndGetNextWatermark(BinlogDmlPo lastElement, long extractedTimestamp) {
                if (lastElement != null) {
                    return new Watermark(getUpdateTime(lastElement));
                }
                return null;
            }

            /**
             * binlog to kafka 时,一个消息中存在多个dml sql,取其中最大的update作为时间戳和watermark
             * @param po
             * @return
             */
            private Long getUpdateTime(BinlogDmlPo po) {
                JSONArray records = po.getRecords();
                Long maxUpdateTime = 0L;
                for (int i = 0; i < records.size(); i++) {
                    String update_time = records.getJSONObject(i).getString("update_time");
                    Long timestamp = 0L;
                    try {
                        timestamp = DateUtil.getTime(update_time);
                    } catch (ParseException e) {
                        log.error("无法转换解析后的update_time为long时间戳");
                        e.printStackTrace();
                    }
                    maxUpdateTime = timestamp > maxUpdateTime ? timestamp : maxUpdateTime;
                }
                return maxUpdateTime == 0 ? null : maxUpdateTime;
            }
        };
    }
}
