package com.realtime.app.po;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;

@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class T_tc_project_invest_order extends BaseTablePo {
    private String id;
    private String status;
    private BigDecimal amount;
    private String deadline;
    private String deadline_unit;
}
