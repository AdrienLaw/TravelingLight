package com.adrien.bigdata.realtime.dvo;

import java.io.Serializable;

/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/26 0026
*@Description 国标地区封装模型
**/
public class RegionDO implements Serializable {

    private String regionCode;
    private String regionCodeDesc;
    private String regionCity;
    private String regionCityDesc;
    private String regionProvince;
    private String regionProvinceDesc;

    public String getRegionCode() {
        return regionCode;
    }

    public void setRegionCode(String regionCode) {
        this.regionCode = regionCode;
    }

    public String getRegionCodeDesc() {
        return regionCodeDesc;
    }

    public void setRegionCodeDesc(String regionCodeDesc) {
        this.regionCodeDesc = regionCodeDesc;
    }

    public String getRegionCity() {
        return regionCity;
    }

    public void setRegionCity(String regionCity) {
        this.regionCity = regionCity;
    }

    public String getRegionCityDesc() {
        return regionCityDesc;
    }

    public void setRegionCityDesc(String regionCityDesc) {
        this.regionCityDesc = regionCityDesc;
    }

    public String getRegionProvince() {
        return regionProvince;
    }

    public void setRegionProvince(String regionProvince) {
        this.regionProvince = regionProvince;
    }

    public String getRegionProvinceDesc() {
        return regionProvinceDesc;
    }

    public void setRegionProvinceDesc(String regionProvinceDesc) {
        this.regionProvinceDesc = regionProvinceDesc;
    }
}
