package com.realtime.app.hbase.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

public class HBaseTableSink {
    private static Logger log = Logger.getLogger(HBaseTableSink.class);

    /**
     * 将dataStream中的元素写入到hbase
     *
     * @param dataStream
     */
    public static void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        dataStream.map(new MapFunction<Tuple2<Boolean, Row>, Object>() {
            @Override
            public Object map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
                //log.info(booleanRowTuple2.toString());
                return null;
            }
        });
    }


}
