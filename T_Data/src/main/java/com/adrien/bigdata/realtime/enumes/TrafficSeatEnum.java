package com.adrien.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;
/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/26 0026
*@Description 交通工具的席别和舱位
**/
public enum TrafficSeatEnum {
    //座席类别(商务座、一等、二等、软卧、硬卧、硬座)
    //舱位(头等舱、经济舱)

    TRAIN_GD_BUSY("10", "tr_busyseat","商务座"),
    TRAIN_GD_FIRST("11", "tr_firstseat","一等"),
    TRAIN_GD_SECOND("12", "tr_secondseat","二等"),
    TRAIN_SOFT_BERTH("13", "tr_secondseat","软卧"),
    TRAIN_HAND_BERTH("14", "tr_secondseat","硬卧"),

    AIRPLAT_BUSY("20", "ap_busyseat","头等舱"),
    AIRPLAT_COMMON("21", "ap_common","经济舱");


    private String code;
    private String desc;
    private String remark;

    private TrafficSeatEnum(String code, String remark, String desc) {
        this.code = code;
        this.remark = remark;
        this.desc = desc;
    }


    public static List<String> getAllTrafficSeats(){
        List<String> traffics = Arrays.asList(
                TRAIN_GD_BUSY.code,
                TRAIN_GD_FIRST.code,
                TRAIN_GD_SECOND.code,
                TRAIN_SOFT_BERTH.code,
                TRAIN_HAND_BERTH.code,

                AIRPLAT_BUSY.code,
                AIRPLAT_COMMON.code
        );
        return traffics;
    }

    public static List<String> getTrainTrafficSeats(){
        List<String> traffics = Arrays.asList(
                TRAIN_GD_BUSY.code,
                TRAIN_GD_FIRST.code,
                TRAIN_GD_SECOND.code,
                TRAIN_SOFT_BERTH.code,
                TRAIN_HAND_BERTH.code
        );
        return traffics;
    }

    public static List<String> getAirTrafficSeats(){
        List<String> traffics = Arrays.asList(
                AIRPLAT_BUSY.code,
                AIRPLAT_COMMON.code
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
