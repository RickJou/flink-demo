package com.realtime.app.deserializer;

import com.alibaba.fastjson.JSONObject;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
public class BinlogRecord {
    private JSONObject record;
    private String databaseName;
    private String tableName;
}
