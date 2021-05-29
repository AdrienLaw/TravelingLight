package com.adrien.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;
/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/26 0026
*@Description 事件枚举，，，主要是针对产品的各种页面
**/
public enum EventEnum {

    VIEW("01", "view","浏览"),
    CLICK("02", "click","点击"),
    INPUT("03", "input","输入"),
    SLIDE("04", "slide","滑动");


    private String code;
    private String desc;
    private String remark;

    private EventEnum(String code, String remark, String desc) {
        this.code = code;
        this.remark = remark;
        this.desc = desc;
    }

    public static List<String> getEvents(){
        List<String> events = Arrays.asList(
                CLICK.code,
                INPUT.code,
                SLIDE.code,
                VIEW.code
        );
        return events;
    }

    public static List<String> getInterActiveEvents(){
        List<String> events = Arrays.asList(
                CLICK.code,
                VIEW.code,
                SLIDE.code
        );
        return events;
    }


    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public String getRemark() {
        return remark;
    }
}
