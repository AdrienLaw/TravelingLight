package com.adrien.bigdata.realtime.util;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/26 0026
*@Description 反射工具类
**/
public class ReflexUtil implements Serializable {

    public static Map<String,Class>  datatypes = new HashMap<String,Class>();
    static {
        datatypes.put(Integer.class.getName(), Integer.class);
        datatypes.put(Long.class.getName(), Long.class);
        datatypes.put(String.class.getName(), String.class);
        datatypes.put(Boolean.class.getName(), Boolean.class);
        datatypes.put(List.class.getName(), List.class);
        datatypes.put(Map.class.getName(), Map.class);
    }

    /**
     *
     */
    private static final long serialVersionUID = 1L;


    public static void setFildKeyValues4SingleString(Object obj, String fieldName, Object fieldValue) throws Exception{
        if(null != obj){
            Class cls = obj.getClass();
            String methodName = "set"+ StringUtils.capitalize(fieldName);
            Method method = cls.getMethod(methodName,String.class);
            method.invoke(obj, fieldValue);
        }
    }

    /**
     * 静态方法反射执行
     * @param clsName
     * @param methodName
     * @return
     * @throws Exception
     */
    public static Object invoke(String clsName,String methodName) throws Exception{
        Class<?> clazz = Class.forName(clsName);
        Method method = clazz.getMethod(methodName);
        return method.invoke(null);
    }

    public static Object invoke(String clsName,String methodName,String paramCls,Object param) throws Exception{
        Class<?> clazz = Class.forName(clsName);
        Class<?> pcls = datatypes.getOrDefault(paramCls, String.class);
        Method method = clazz.getMethod(methodName,pcls);
        return method.invoke(null,param);
    }

//    public static void main(String[] args) throws Exception {
//
//       Object obj = invoke("com.bigdata.demo.offine.util.CommonUtil","getRandomName");
//       System.out.println(obj);
//
//        Object obj2 = invoke("com.bigdata.demo.offine.util.CommonUtil","getRandom","int",10);
//        System.out.println(obj2);
//
//    }

}

