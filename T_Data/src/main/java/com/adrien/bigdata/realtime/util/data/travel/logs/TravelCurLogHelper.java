package com.adrien.bigdata.realtime.util.data.travel.logs;

import com.adrien.bigdata.realtime.constant.CommonConstant;
import com.adrien.bigdata.realtime.dvo.GisDO;
import com.adrien.bigdata.realtime.enumes.*;
import com.adrien.bigdata.realtime.util.AmapGisUtil;
import com.adrien.bigdata.realtime.util.CommonUtil;
import com.adrien.bigdata.realtime.util.QParameterTool;
import com.adrien.bigdata.realtime.util.kafka.producer.KafkaProducerUtil;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

import java.time.temporal.ChronoUnit;
import java.util.*;
/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/26 0026
*@Description 用户当前旅游行为日志模拟构造器
**/
public class TravelCurLogHelper {

    //=====基础信息==============================================================
    //kafka分区Key
    public static final String KEY_KAFKA_ID = "KAFKA_ID";

    //请求ID
    public static final String KEY_SID = "sid";
    //用户ID
    public static final String KEY_USER_ID = "userID";
    //用户设备号
    public static final String KEY_USER_DEVICE = "userDevice";

    //用户设备类型
    public static final String KEY_USER_DEVICE_TYPE = "userDeviceType";
    //操作系统
    public static final String KEY_OS = "os";
    //手机制造商
    public static final String KEY_MANUFACTURER = "manufacturer";
    //电信运营商
    public static final String KEY_CARRIER = "carrier";
    //网络类型
    public static final String KEY_NETWORK_TYPE = "networkType";
    //用户所在地区
    public static final String KEY_USER_REGION = "userRegion";
    //用户所在地区IP
    public static final String KEY_USER_REGION_IP = "userRegionIP";
    //经度
    public static final String KEY_LONGITUDE = "lonitude";
    //纬度
    public static final String KEY_LATITUDE = "latitude";



    //行为类型
    public static final String KEY_ACTION = "action";

    //事件类型
    public static final String KEY_EVENT_TYPE = "eventType";

    //事件目的
    public static final String KEY_EVENT_TARGET_TYPE = "eventTargetType";



    //停留时长
    public static final String KEY_DURATION = "duration";

    public static final Integer KEY_DURATION_PAGE_NAGV_MIN = 3;
    public static final Integer KEY_DURATION_PAGE_NAGV_MAX = 90;

    public static final Integer KEY_DURATION_PAGE_VIEW_MIN = 3;
    public static final Integer KEY_DURATION_PAGE_VIEW_MAX = 60;

    //创建时间
    public static final String KEY_CT = "ct";

    //=====扩展信息==============================================================

    public static final String KEY_EXTS = "exts";

    //产品ID
    public static final String KEY_PRODUCT_ID = "productID";

    //产品列表
    public static final String KEY_PRODUCT_IDS = "productIDS";

    //目标信息
    public static final String KEY_EXTS_TARGET_ID = "targetID";
    public static final String KEY_EXTS_TARGET_IDS = "targetIDS";
    public static final String KEY_EXTS_TARGET_KEYS = "targetKeys";

    //=====查询信息==============================================================

    public static final String KEY_EXTS_QUERY_TRAVEL_TIME = "travelTime";//行程天数
    public static final String KEY_EXTS_QUERY_HOT_TARGET = "hotTarget";//热门目的地
    public static final String KEY_EXTS_QUERY_SEND = "travelSend";//出发地
    public static final String KEY_EXTS_QUERY_SEND_TIME = "travelSendTime";//出发时间
    public static final String KEY_EXTS_QUERY_PRODUCT_TYPE = "productType";//产品类型：跟团、私家、半自助
    public static final String KEY_EXTS_QUERY_PRODUCT_LEVEL = "productLevel";//产品钻级


    //用户数量限制级别
    public static final Integer USER_COUNT_LEVEL = 5;


    //地区信息
    public static List<GisDO> giss = new ArrayList<GisDO>();
    static {
        giss = AmapGisUtil.initDatas();
    }


    /**
     * 模拟旅游行业用户行为日志
     * @return
     */
    public static Map<String,Object> getTravelCommonData(String selectAction, String curTime, int countLevel, List<GisDO> giss, List<String> productIDs) throws Exception{

        Map<String,Object> result = new HashMap<String,Object>();

        //请求id
        String sid = CommonUtil.getRandomChar(USER_COUNT_LEVEL);
        String sidNum = curTime+sid;
        result.put(KEY_SID, sidNum);

        //用户ID
        String userId = CommonUtil.getRandomNumStr(countLevel);
        result.put(KEY_USER_ID, userId);

        //用户设备号
        String userDevice = CommonUtil.getRandomNumStr(countLevel);
        result.put(KEY_USER_DEVICE, userDevice);

        //用户设备类型
        String userDeviceType = CommonUtil.getRandomElementRange(DeviceTypeEnum.getDeviceTypes());
        result.put(KEY_USER_DEVICE_TYPE, userDeviceType);

        //操作系统
        String os = CommonUtil.getRandomElementRange(OSEnum.getOS());
        result.put(KEY_OS, os);

        //手机制造商
        String manufacturer = CommonUtil.getRandomElementRange(ManufacturerEnum.getManufacturers());
        result.put(KEY_MANUFACTURER, manufacturer);

        //电信运营商
        String carrier = CommonUtil.getRandomElementRange(CarrierEnum.getCarriers());
        result.put(KEY_CARRIER, carrier);

        //网络类型
        String networkType = CommonUtil.getRandomElementRange(NetworkTypeEnum.getNetworkTypes());
        result.put(KEY_NETWORK_TYPE, networkType);

        //用户所在地区
        GisDO gisDO = CommonUtil.getRandomElementRange(giss);
        result.put(KEY_USER_REGION, gisDO.getAdcode());

        //经度|纬度
        result.put(KEY_LONGITUDE, gisDO.getLongitude());
        result.put(KEY_LATITUDE, gisDO.getLatitude());

        //用户所在地区IP
        result.put(KEY_USER_REGION_IP, CommonUtil.getRadomIP());

        //行为类型
        List<String> midActions = ActionEnum.getMidActions();
        String action = CommonUtil.getRandomElementRange(midActions);
        if(StringUtils.isNotEmpty(selectAction) && midActions.contains(selectAction)){
            action = selectAction;
        }
        result.put(KEY_ACTION, action);

        //事件类型
        String eventType = "";
        if(ActionEnum.INTERACTIVE.getCode().equalsIgnoreCase(action)){
            eventType = CommonUtil.getRandomElementRange(EventEnum.getInterActiveEvents());
            //eventType = EventEnum.CLICK.getCode();
        }else if(ActionEnum.PAGE_ENTER_NATIVE.getCode().equalsIgnoreCase(action)){
            eventType = EventEnum.VIEW.getCode();
        }
        result.put(KEY_EVENT_TYPE, eventType);


        //时长处理
        int duration = 0;
        if(ActionEnum.PAGE_ENTER_NATIVE.getCode().equalsIgnoreCase(action)){
            //页面浏览
            List<Integer> durations = CommonUtil.getRangeNumeric(KEY_DURATION_PAGE_NAGV_MIN, KEY_DURATION_PAGE_NAGV_MAX,  1);
            duration = CommonUtil.getRandomElementRange(durations);
        }else if(ActionEnum.INTERACTIVE.getCode().equalsIgnoreCase(action)){
            if(EventEnum.VIEW.getCode().equalsIgnoreCase(eventType) || EventEnum.SLIDE.getCode().equalsIgnoreCase(eventType)){
                //页面比较
                List<Integer> durations = CommonUtil.getRangeNumeric(KEY_DURATION_PAGE_VIEW_MIN, KEY_DURATION_PAGE_VIEW_MAX,  1);
                duration = CommonUtil.getRandomElementRange(durations);
            }
        }
        result.put(KEY_DURATION, String.valueOf(duration));


        //创建时间
        long ct = CommonUtil.getSelectTimestamp(curTime, CommonConstant.FORMATTER_YYYYMMDDHHMMDD);
        result.put(KEY_CT, ct);

        //hashMD5
        String kafkaKey = CommonUtil.getRandom(10);
        result.put(KEY_KAFKA_ID, kafkaKey);

        return result;
    }


    /**
     * 模拟旅游行业用户行为日志
     * @return
     */
    public static Map<String,Object> getTravelExtData(Map<String,Object> commonData, List<GisDO> giss, List<String> productIDs) throws Exception{

        Map<String,Object> result = new HashMap<String,Object>();

        //行为类型
        String action = commonData.getOrDefault(KEY_ACTION,"").toString();
        //事件类型
        String eventType = commonData.getOrDefault(KEY_EVENT_TYPE,"").toString();

        //扩展信息
        Map<String,String> subDatas = new HashMap<String,String>();
        if(ActionEnum.LAUNCH.getCode().equalsIgnoreCase(action)){
            //无具体扩展信息
        }else if(ActionEnum.PAGE_ENTER_NATIVE.getCode().equalsIgnoreCase(action) || ActionEnum.PAGE_ENTER_H5.getCode().equalsIgnoreCase(action)){
            //页面浏览
            //产品ID
            String productID = CommonUtil.getRandomElementRange(productIDs);
            subDatas.put(KEY_EXTS_TARGET_ID, productID);
        }else if(ActionEnum.INTERACTIVE.getCode().equalsIgnoreCase(action)){
            //点击事件
            if(EventEnum.CLICK.getCode().equalsIgnoreCase(eventType)){
                //点击事件
                //产品ID页面
                String productID = CommonUtil.getRandomElementRange(productIDs);
                subDatas.put(KEY_EXTS_TARGET_ID, productID);

                //事件类型
                String eventAction = CommonUtil.getRandomElementRange(EventActionEnum.getEventActions());
                subDatas.put(KEY_EVENT_TARGET_TYPE, eventAction);

            }else if(EventEnum.VIEW.getCode().equalsIgnoreCase(eventType) || EventEnum.SLIDE.getCode().equalsIgnoreCase(eventType)){
                //查询或滑动
                List<String> productIDList = CommonUtil.getRandomSubElementRange(productIDs, 4);
                String productIDListJson = JSON.toJSONString(productIDList);
                subDatas.put(KEY_EXTS_TARGET_IDS, productIDListJson);

                //查询条件
                //行程天数
                List<String> travelTimes = CommonUtil.getRangeNumber(1, 10, 2);
                String travelTime = CommonUtil.getRandomElementRange(travelTimes);
                subDatas.put(KEY_EXTS_QUERY_TRAVEL_TIME, travelTime);

                //热门目的地
                GisDO gisDO = CommonUtil.getRandomElementRange(giss);
                result.put(KEY_EXTS_QUERY_HOT_TARGET, gisDO.getAdcode());

                //出发地
                String userRegion = commonData.getOrDefault(KEY_USER_REGION,"").toString();
                subDatas.put(KEY_EXTS_QUERY_SEND, userRegion);

                //出发时间
                String ctTxt = commonData.getOrDefault(KEY_CT,"").toString();
                Long ct = Long.valueOf(ctTxt);
                String sendTime = CommonUtil.formatDate4Timestamp(CommonUtil.getRandomTimestamp4Release(ct, Calendar.DAY_OF_MONTH, 100), "yyyyMM");
                subDatas.put(KEY_EXTS_QUERY_SEND_TIME, sendTime);

                //产品类型：跟团、私家、半自助
                String tpType = CommonUtil.getRandomElementRange(TravelProductTypeEnum.getTravelProductTypes());
                subDatas.put(KEY_EXTS_QUERY_PRODUCT_TYPE, tpType);

                //产品钻级
                List<String> levels = CommonUtil.getRangeNumber(1, 5, 1);
                String level = CommonUtil.getRandomElementRange(levels);
                subDatas.put(KEY_EXTS_QUERY_PRODUCT_LEVEL, level);
            }
        }
        result.put(KEY_EXTS, JSON.toJSONString(subDatas));

        return result;
    }



    /**
     * 模拟旅游行业用户行为日志
     * @return
     */
    public static Map<String,Object> getTravelDWExtData(Map<String,Object> commonData, String areaCode, String productID) throws Exception{

        Map<String,Object> result = new HashMap<String,Object>();
        result.putAll(commonData);

        //行为类型
        String action = commonData.getOrDefault(KEY_ACTION,"").toString();
        //事件类型
        String eventType = commonData.getOrDefault(KEY_EVENT_TYPE,"").toString();

        //扩展信息
        if(ActionEnum.LAUNCH.getCode().equalsIgnoreCase(action)){
            //无具体扩展信息
        }else if(ActionEnum.PAGE_ENTER_NATIVE.getCode().equalsIgnoreCase(action)){
            //页面浏览
            //产品ID
            result.put(KEY_EXTS_TARGET_ID, productID);
        }else if(ActionEnum.INTERACTIVE.getCode().equalsIgnoreCase(action)){
            //点击事件
            if(EventEnum.CLICK.getCode().equalsIgnoreCase(eventType)){
                //点击事件
                //产品ID
                result.put(KEY_EXTS_TARGET_ID, productID);

                //事件类型
                String eventAction = CommonUtil.getRandomElementRange(EventActionEnum.getEventActions());
                result.put(KEY_EVENT_TARGET_TYPE, eventAction);

            }else if(EventEnum.VIEW.getCode().equalsIgnoreCase(eventType) || EventEnum.SLIDE.getCode().equalsIgnoreCase(eventType)){
                //查询或滑动
                result.put(KEY_EXTS_TARGET_ID, productID);

                //查询条件
                //行程天数
                List<String> travelTimes = CommonUtil.getRangeNumber(1, 10, 2);
                String travelTime = CommonUtil.getRandomElementRange(travelTimes);
                result.put(KEY_EXTS_QUERY_TRAVEL_TIME, travelTime);

                //热门目的地
                result.put(KEY_EXTS_QUERY_HOT_TARGET, areaCode);

                //出发地
                String userRegion = commonData.getOrDefault(KEY_USER_REGION,"").toString();
                result.put(KEY_EXTS_QUERY_SEND, userRegion);

                //出发时间
                String ctTxt = commonData.getOrDefault(KEY_CT,"").toString();
                Long ct = Long.valueOf(ctTxt);
                String sendTime = CommonUtil.formatDate4Timestamp(CommonUtil.getRandomTimestamp4Release(ct, Calendar.DAY_OF_MONTH, 100), "yyyyMM");
                result.put(KEY_EXTS_QUERY_SEND_TIME, sendTime);

                //产品类型：跟团、私家、半自助
                String tpType = CommonUtil.getRandomElementRange(TravelProductTypeEnum.getTravelProductTypes());
                result.put(KEY_EXTS_QUERY_PRODUCT_TYPE, tpType);

                //产品钻级
                List<String> levels = CommonUtil.getRangeNumber(1, 5, 1);
                String level = CommonUtil.getRandomElementRange(levels);
                result.put(KEY_EXTS_QUERY_PRODUCT_LEVEL, level);
            }
        }
        return result;
    }


    /**
     * 模拟旅游行业用户行为日志
     * @return
     */
    public static Map<String,Object> getTravelODSData(String selectAction, String curTime, int countLevel, List<GisDO> giss, List<String> productIDs) throws Exception{

        Map<String,Object> result = new HashMap<String,Object>();

        //基础数据
        Map<String,Object> commonData = getTravelCommonData(selectAction, curTime, countLevel, giss, productIDs);
        result.putAll(commonData);

        Map<String,Object> extsData = getTravelExtData(commonData, giss, productIDs);
        result.putAll(extsData);

        return result;
    }


    /**
     * 模拟旅游行业用户行为日志
     * @return
     */
    public static Map<String,Object> getTravelODSDatas(String selectAction,String dts, int count) throws Exception{
        Map<String,Object> result = new HashMap<String,Object>();

        //单条记录信息
        List<String> productIDs = CommonUtil.getProducts();
        for(int i=1; i<=count; i++){
            Map<String,Object> subDatas = getTravelODSData(selectAction, dts, USER_COUNT_LEVEL, giss, productIDs);
            result.putAll(subDatas);
        }
        return result;
    }


    /**
     * 模拟旅游行业用户行为日志
     * @return
     */
    public static List<Map<String,Object>> getTravelDWData(String selectAction,String curTime, int countLevel, List<GisDO> giss, List<String> productIDs) throws Exception{

        List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();

        //基础数据
        Map<String,Object> commonData = getTravelCommonData(selectAction, curTime, countLevel, giss, productIDs);

        //查询或滑动
        List<String> productIDList = CommonUtil.getRandomSubElementRange(productIDs, 4);
        for(String productID : productIDList){
            GisDO gisDO = CommonUtil.getRandomElementRange(giss);
            String areaCode = gisDO.getAdcode();

            Map<String,Object> allData = getTravelDWExtData(commonData, areaCode, productID);
            result.add(allData);
        }
        return result;
    }


    /**
     * 模拟旅游行业用户行为日志
     * @return
     */
    public static List<Map<String,Object>> getTravelDWDatas(String selectAction, String dts, int count) throws Exception{
        List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();

        //单条记录信息
        List<String> productIDs = CommonUtil.getProducts();
        for(int i=1; i<=count; i++){
            List<Map<String,Object>> subDatas = getTravelDWData(selectAction, dts, USER_COUNT_LEVEL, giss, productIDs);
            result.addAll(subDatas);
        }
        return result;
    }



    /**
     * 测试原始数据
     * @param topic
     * @param count
     * @param sleep
     * @throws Exception
     */
    public static void testODSData(String topic, String selectAction, int count,long sleep) throws Exception{
        //发送序列化对象
        String dateFormatter = CommonConstant.FORMATTER_YYYYMMDDHHMMDD;
        String dayFormatter = CommonConstant.FORMATTER_YYYYMMDD;
        ChronoUnit chronoUnit = ChronoUnit.MINUTES;
        ChronoUnit dayChronoUnit = ChronoUnit.DAYS;

        //时间(天)范围轨迹数据
        while(true){
            String curTime = CommonUtil.formatDate(new Date(), dateFormatter);
            Map<String,String> totalDatas = new HashMap<String,String>();

            for(int y=1; y<count; y++){
                Map<String,Object> data = getTravelODSDatas(selectAction, curTime, count);
                String kafkaKey = data.getOrDefault(KEY_KAFKA_ID,"").toString();
                String dataJson = JSON.toJSONString(data);
                System.out.println("dataJson=" + dataJson);


                totalDatas.put(kafkaKey, dataJson);
            }
            KafkaProducerUtil.sendMsg(CommonConstant.KAFKA_PRODUCER_JSON_PATH, topic, totalDatas);
            System.out.println("kafka producer send =" + CommonUtil.formatDate4Def(new Date()));
            Thread.sleep(sleep);
            totalDatas.clear();
        }
    }



    /**
     * 测试清洗数据
     * @param topic
     * @param count
     * @param beginDay
     * @param endDay
     * @param dayBegin
     * @param dayEnd
     * @param sleep
     * @throws Exception
     */
    public static void testDWDataOld(String topic, String selectAction, int count,String beginDay, String endDay, String dayBegin,String dayEnd,long sleep) throws Exception{
        //发送序列化对象
        String dateFormatter = CommonConstant.FORMATTER_YYYYMMDDHHMMDD;
        String dayFormatter = CommonConstant.FORMATTER_YYYYMMDD;
        ChronoUnit chronoUnit = ChronoUnit.MINUTES;
        ChronoUnit dayChronoUnit = ChronoUnit.DAYS;

        //时间(天)范围轨迹数据
        int diffDay = CommonUtil.calculateTimeDiffDay(dayFormatter, beginDay, endDay, Calendar.DATE).intValue();
        for(int i=0;i<=diffDay;i++){
            Map<String,String> totalDatas = new HashMap<String,String>();

            String curDay = CommonUtil.computeFormatTime(beginDay, i, Calendar.DATE, dayFormatter);
            String btStr = curDay+dayBegin;
            String etStr = curDay+dayEnd;

            //每天的轨迹数据
            int diff = CommonUtil.calculateTimeDiffDay(dateFormatter, btStr, etStr, Calendar.SECOND).intValue();
            for(int z=0;z<=diff;z++){
                for(int y=1; y<count; y++){
                    String curTime = CommonUtil.computeFormatTime(btStr, z, Calendar.SECOND, dateFormatter);
                    List<Map<String,Object>> datas = getTravelDWDatas(selectAction, curTime, count);
                    for(Map<String,Object> data : datas){
                        String kafkaKey = data.getOrDefault(KEY_KAFKA_ID,"").toString();
                        String dataJson = JSON.toJSONString(data);
                        //System.out.println("dataJson=>" + dataJson);
                        totalDatas.put(kafkaKey, dataJson);
                    }
                }

                //String datas = JSON.toJSONString(totalDatas);
                //System.out.println("dw.data send =" + datas);

                KafkaProducerUtil.sendMsg(CommonConstant.KAFKA_PRODUCER_JSON_PATH, topic, totalDatas);
                System.out.println("kafka producer send =" + CommonUtil.formatDate4Def(new Date()));
                Thread.sleep(sleep);
                totalDatas.clear();
            }

            KafkaProducerUtil.sendMsg(CommonConstant.KAFKA_PRODUCER_JSON_PATH, topic, totalDatas);
            System.out.println("kafka producer send =" + CommonUtil.formatDate4Def(new Date()));
        }
    }



    /**
     * 测试清洗数据
     * @param topic
     * @param count
     * @param sleep
     * @throws Exception
     */
    public static void testDWData(String topic, String selectAction, int count,long sleep) throws Exception{
        //发送序列化对象
        String dateFormatter = CommonConstant.FORMATTER_YYYYMMDDHHMMDD;
        String dayFormatter = CommonConstant.FORMATTER_YYYYMMDD;
        ChronoUnit chronoUnit = ChronoUnit.MINUTES;
        ChronoUnit dayChronoUnit = ChronoUnit.DAYS;


        //时间(天)范围轨迹数据
        while(true){
            String curTime = CommonUtil.formatDate(new Date(), dateFormatter);
            Map<String,String> totalDatas = new HashMap<String,String>();

            for(int y=1; y<count; y++){
                List<Map<String,Object>> datas = getTravelDWDatas(selectAction, curTime, count);
                for(Map<String,Object> data : datas){
                    String kafkaKey = data.getOrDefault(KEY_KAFKA_ID,"").toString();
                    String dataJson = JSON.toJSONString(data);
                    //System.out.println("dataJson=>" + dataJson);
                    totalDatas.put(kafkaKey, dataJson);
                }
            }
            KafkaProducerUtil.sendMsg(CommonConstant.KAFKA_PRODUCER_JSON_PATH, topic, totalDatas);
            System.out.println("kafka producer send =" + CommonUtil.formatDate4Def(new Date()));
            Thread.sleep(sleep);
            totalDatas.clear();
        }
    }



    /**
     * 选择造数
     * @param args
     * @throws Exception
     */
    public static void chooseFun(String[] args) throws Exception{
        QParameterTool tools = QParameterTool.fromArgs(args);

        String topic = tools.get(KEY_TOPIC);
        String source = tools.get(KEY_SOURCE);
        String action = tools.get(KEY_ACTION);

        Integer count = tools.getInt(KEY_COUNT);
        Integer sleep = tools.getInt(KEY_SLEEP);

        if(SOURCE_ODS.equalsIgnoreCase(source)){
            testODSData(topic, action, count, sleep);

        }else if(SOURCE_DW.equalsIgnoreCase(source)){
            testDWData(topic, action, count, sleep);
        }


    }

    //=====参数==================================================================

    public static final String KEY_TOPIC = "topic";
    public static final String KEY_SOURCE = "source";
    public static final String KEY_BEGIN = "begin";
    public static final String KEY_END = "end";
    public static final String KEY_COUNT = "count";
    public static final String KEY_SLEEP = "sleep";

    public static final String SOURCE_ODS = "ods";
    public static final String SOURCE_DW = "dw";

    public static final String TIME_ORDER = "time_order";

    public static final Integer TIME_RANGE_MIN = 1;
    public static final Integer TIME_RANGE_MAX = 120;

    public static final Integer COUNT_MIN = 1;
    public static final Integer COUNT_MAX = 10000;

    public static final Integer SLEEP_MIN = 1000 * 1;
    public static final Integer SLEEP_MAX = 1000 * 60;



    /**
     * 参数校验
     * @param args
     * @return
     */
    private static String checkParams(String[] args) {
        String result = "";
        QParameterTool tools = QParameterTool.fromArgs(args);
        String topic = tools.get(KEY_TOPIC);
        if(StringUtils.isEmpty(topic)){
            result = "topic is empty!";
            return result;
        }

        String source = tools.get(KEY_SOURCE);
        if(!SOURCE_ODS.equalsIgnoreCase(source) && !SOURCE_DW.equalsIgnoreCase(source)){
            source = "source is error['ods','dw']!";
            return result;
        }

        Integer count = tools.getInt(KEY_COUNT);
        if(null == count){
            result = "count is empty!";
            return result;
        }else {
            if(count > COUNT_MAX || count < COUNT_MIN){
                result = "count is unbound["+COUNT_MIN+","+COUNT_MAX+"]!";
                return result;
            }
        }
        Integer sleep = tools.getInt(KEY_SLEEP);
        if(null == sleep){
            result = "sleep is empty!";
            return result;
        }else {
            if(sleep > SLEEP_MAX || sleep < SLEEP_MIN){
                result = "sleep is unbound["+SLEEP_MIN+","+SLEEP_MAX+"]!";
                return result;
            }
        }

        return result;
    }


    /**
     * 造数入口
     * 示例： --topic test_logs --source ods --action 02 --count 3 --sleep 3000
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
        //运行参数示例
//        String s = "--topic test_logs --source ods --action 02 --count 3 --sleep 3000";
//        String[] args2 = s.split(" ");

        String checkResult = checkParams(args);
        if(StringUtils.isEmpty(checkResult)){
            chooseFun(args);
        }else{
            System.out.println(checkResult);
        }
    }
}
