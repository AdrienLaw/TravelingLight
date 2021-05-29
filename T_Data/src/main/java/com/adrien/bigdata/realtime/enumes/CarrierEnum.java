package com.adrien.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;
/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/26 0026
*@Description 通讯运营商枚举
**/
public enum CarrierEnum {

    CHINA_MOBILE("1", "中国移动"),
    CHINA_UNICOM("2", "中国联通"),
    CHINA_TElECOM("3", "中国电信");


    private String code;
    private String desc;

    private CarrierEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<String> getCarriers(){
        List<String> carriers = Arrays.asList(
                CHINA_MOBILE.code,
                CHINA_UNICOM.code,
                CHINA_TElECOM.code
        );
        return carriers;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
