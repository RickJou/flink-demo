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
import java.text.ParseException;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class BaseTablePo {
    private String update_time;

    public String getUpdateDay() {//更新时间:天
        return update_time.substring(0, 10);
    }

    /**
     * 修改时间:时间戳
     * @return
     */
    public Long getLong_update_time(){
        try {
            return DateUtil.getTime(update_time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }


    /***
     * 获取通用表update作为时间戳和水位
     * @return
     */
    public AssignerWithPunctuatedWatermarks getCommonRecordAssignerWithPunctuatedWatermarks() {
        return new AssignerWithPunctuatedWatermarks<BaseTablePo>() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(BaseTablePo po, long l) {
                try {
                    return new Watermark(DateUtil.getTime(BeanUtils.getProperty(po, "update_time")));
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
                    return DateUtil.getTime(BeanUtils.getProperty(po, "update_time"));
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
