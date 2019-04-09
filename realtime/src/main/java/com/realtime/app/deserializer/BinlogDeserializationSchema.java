package com.realtime.app.deserializer;

import com.realtime.app.kafka.convert.BinlogConvert;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

/**
 * binlog record 反序列化专用
 */
@PublicEvolving
public class BinlogDeserializationSchema extends AbstractDeserializationSchema<BinlogDmlPo> {

    @Override
    public BinlogDmlPo deserialize(byte[] message) throws IOException {
        return BinlogConvert.getCUDRecord(new String(message));
    }
}
