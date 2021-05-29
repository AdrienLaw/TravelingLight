package com.adrien.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;
/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/26 0026
*@Description 访问应用产品设备类型枚举
**/
public enum DeviceTypeEnum {

    ANDROID("1", "android"),
    IOS("2", "IOS"),
    OTHER("9", "other");  //可以将其看着是pc


    private String code;
    private String desc;

    private DeviceTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }


    public static List<String> getDeviceTypes(){
        List<String> actions = Arrays.asList(
                ANDROID.code,
                IOS.code,
                OTHER.code
        );
        return actions;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
