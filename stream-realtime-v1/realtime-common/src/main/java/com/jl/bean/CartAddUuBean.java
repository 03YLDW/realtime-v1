package com.jl.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package bean.CartAddUuBean
 * @Author jia.le
 * @Date 2025/4/15 19:03
 * @description:
 */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}
