package com.adrien.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;
/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/26 0026
*@Description 旅游产品类型枚举
**/
public enum TravelProductTypeEnum {

    FOLLOW_TEAM("01", "follow","跟团"),
    PRIVATE("02", "follow","私家"),
    SELF_HELP("03", "self_help","半自助");


    private String code;
    private String desc;
    private String remark;

    private TravelProductTypeEnum(String code, String remark, String desc) {
        this.code = code;
        this.remark = remark;
        this.desc = desc;
    }


    public static List<String> getTravelProductTypes(){
        List<String> codes = Arrays.asList(
                FOLLOW_TEAM.code,
                PRIVATE.code,
                SELF_HELP.code
        );
        return codes;
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
