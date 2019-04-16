package com.realtime.app.po;

import com.realtime.app.util.DateUtil;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class BaseTablePo {
    private Timestamp update_time;

    private String update_day;//更新时间:天

    private Timestamp create_time;

    private String create_day;//创建时间:天

    public String getUpdate_day() {
        return getUpdateDay();
    }

    public String getCreate_day() {
        return getCreateDay();
    }

    private String getUpdateDay() {
        if (update_day == null) {
            Date date = new Date(this.update_time.getTime());
            this.update_day = DateUtil.getDate(date);
        }
        return update_day;
    }

    private String getCreateDay() {
        if (create_day == null) {
            Date date = new Date(this.create_time.getTime());
            this.create_day = DateUtil.getDate(date);
        }
        return create_day;
    }

    /**
     * 修改时间:时间戳
     *
     * @return
     */
    public Long getLong_update_time() {
        return update_time.getTime();
    }


    /***
     * 获取通用表update作为时间戳和水位
     * @return
     */
    public AssignerWithPunctuatedWatermarks getCommonRecordAssignerWithPunctuatedWatermarks(String timeFieldName) {
        return new AssignerWithPunctuatedWatermarks<BaseTablePo>() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(BaseTablePo po, long l) {
                try {
                    return new Watermark(DateUtil.getUtcAdd8HourTime(BeanUtils.getProperty(po, timeFieldName)));
                } catch (ParseException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public long extractTimestamp(BaseTablePo po, long l) {
                try {
                    return DateUtil.getUtcAdd8HourTime(BeanUtils.getProperty(po, timeFieldName));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return 0L;
            }
        };
    }
}
