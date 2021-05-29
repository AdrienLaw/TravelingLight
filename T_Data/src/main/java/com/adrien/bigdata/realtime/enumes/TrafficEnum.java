package com.adrien.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;
/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/26 0026
*@Description 旅游选择的交通工具类型枚举
**/
public enum TrafficEnum {
    GTRAIN("01", "gtrain","高铁"),
    CTRAIN("02", "ctrain","火车"),
    AIRPLAN("03", "airplan","飞机"),
    STEAMSHIP("04", "steamship","轮船");


    private String code;
    private String desc;
    private String remark;

    private TrafficEnum(String code, String remark, String desc) {
        this.code = code;
        this.remark = remark;
        this.desc = desc;
    }


    public static List<String> getAllTraffics(){
        List<String> traffics = Arrays.asList(
                GTRAIN.code,
                CTRAIN.code,
                AIRPLAN.code,
                STEAMSHIP.code
        );
        return traffics;
    }

    public static List<String> getCommonTraffics(){
        List<String> traffics = Arrays.asList(
                GTRAIN.code,
                CTRAIN.code,
                AIRPLAN.code
        );
        return traffics;
    }

    public static List<String> getGoodTraffics(){
        List<String> traffics = Arrays.asList(
                GTRAIN.code,
                AIRPLAN.code
        );
        return traffics;
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
