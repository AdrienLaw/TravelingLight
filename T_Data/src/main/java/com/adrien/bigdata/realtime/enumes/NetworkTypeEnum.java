package com.adrien.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;
/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/26 0026
*@Description 访问应用使用的网络类型枚举
**/
public enum NetworkTypeEnum {

    WIFI("0", "无线"),
    D4G("1", "4g"),
    D3G("2", "3g"),
    OFFLINE("3", "线下支付");



    private String code;
    private String desc;

    private NetworkTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<String> getNetworkTypes(){
        List<String> networkTypes = Arrays.asList(
                D4G.code,
                D3G.code,
                WIFI.code,
                OFFLINE.code
        );
        return networkTypes;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
