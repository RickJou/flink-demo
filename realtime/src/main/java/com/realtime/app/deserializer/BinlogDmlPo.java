package com.realtime.app.deserializer;

import com.alibaba.fastjson.JSONArray;
import lombok.Data;

@Data
public class BinlogDmlPo {
    private JSONArray records;
    private String databaseName;
    private String tableName;
}
