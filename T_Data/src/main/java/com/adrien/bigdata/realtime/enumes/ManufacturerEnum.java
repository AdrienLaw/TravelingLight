package com.adrien.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;
/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/26 0026
*@Description 手机生产商枚举 ---使用什么样类型的手机
**/
public enum ManufacturerEnum {

    HUAWEI("01", "华为"),
    OPPO("02", "oppo"),
    VIVO("03", "vivo"),
    XIAOMI("04", "小米"),
    SAMSUNG("05", "三星"),
    APPLE("06", "苹果"),
    LENOVO("07", "联想"),
    SONY("08", "索尼"),
    NOKIA("09", "诺基亚"),
    ZTE("10", "中信");


    private String code;
    private String desc;

    private ManufacturerEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<String> getManufacturers(){
        List<String> manufacturers = Arrays.asList(
                HUAWEI.code,
                OPPO.code,
                VIVO.code,
                XIAOMI.code,
                SAMSUNG.code,
                APPLE.code,
                LENOVO.code,
                SONY.code,
                NOKIA.code,
                ZTE.code
        );
        return manufacturers;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
