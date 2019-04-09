package com.realtime.app.kafka.convert;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.realtime.app.deserializer.BinlogDmlPo;

import java.util.Arrays;

public class BinlogConvert {

    private static String INSERT = "INSERT";
    private static String UPDATE = "UPDATE";
    private static String DELETE = "DELETE";
    private static String CREATE = "CREATE";
    private static String ALTER = "ALTER";


    private static String TYPE = "type";
    private static String DATA = "data";
    private static String SQL = "sql";
    private static String DATABASE = "database";
    private static String TABLE = "table";

    /**
     * 获取insert和update类型的消息
     */
    public static BinlogDmlPo getAddAndModifyRecord(String record) {
        return getRecordByType(record, INSERT, UPDATE);
    }

    public static BinlogDmlPo getDelRecord(String record) {
        return getRecordByType(record, DELETE);
    }

    public static BinlogDmlPo getCUDRecord(String record) {
        return getRecordByType(record, INSERT, UPDATE, DELETE);
    }


    private static BinlogDmlPo getRecordByType(String record, String... types) {
        JSONObject recordJSObj = JSON.parseObject(record);
        String msgType = recordJSObj.getString(TYPE);
        if (Arrays.asList(types).contains(msgType)) {
            BinlogDmlPo po = new BinlogDmlPo();
            po.setRecords(recordJSObj.getJSONArray(DATA));
            po.setDatabaseName(recordJSObj.getString(DATABASE));
            po.setTableName(recordJSObj.getString(TABLE));
            return po;
        }
        return null;
    }

}

