package com.adrien.bigdata.realtime.util.data.travel.orders;



import com.adrien.bigdata.realtime.constant.CommonConstant;
import com.adrien.bigdata.realtime.dvo.RegionDO;
import com.adrien.bigdata.realtime.enumes.TrafficEnum;
import com.adrien.bigdata.realtime.enumes.TrafficGoBackEnum;
import com.adrien.bigdata.realtime.enumes.TrafficSeatEnum;
import com.adrien.bigdata.realtime.util.CSVUtil;
import com.adrien.bigdata.realtime.util.CommonUtil;
import com.adrien.bigdata.realtime.util.QParameterTool;
import com.adrien.bigdata.realtime.util.kafka.producer.KafkaProducerUtil;
import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/26 0026
*@Description 旅游产品订单相关实时数据模拟构造器
**/
public class TravelOrderHelper implements Serializable {


    private final static Logger log = LoggerFactory.getLogger(TravelOrderHelper.class);

    //kafka分区Key
    public static final String KEY_KAFKA_ID = "KAFKA_ID";

    //订单编号ID: 时间戳+产品ID+用户ID
    public static final String KEY_ORDER_ID = "order_id";

    //用户ID：(在一些场景下，平台会为用户构造的唯一编号)
    public static final String KEY_USER_ID = "user_id";

    //预留手机号
    public static final String KEY_USER_MOBILE = "user_mobile";

    //旅游产品编号：
    public static final String KEY_PRODUCT_ID = "product_id";

    //旅游产品交通资源：旅游交通选择:飞机|高铁|火车 (出行方式)
    public static final String KEY_PRODUCT_TRAFFIC = "product_traffic";


    //旅游产品交通资源：旅游交通选择:商务|一等|软卧... (座席)
    public static final String KEY_PRODUCT_TRAFFIC_GRADE = "product_traffic_grade";


    //单程|往返
    public static final String KEY_PRODUCT_TRAFFIC_TYPE = "product_traffic_type";

    //旅游产品住宿资源：
    public static final String KEY_PRODUCT_PUB = "product_pub";

    //所在区域：
    public static final String KEY_USER_REGION = "user_region";

    //人员构成_成人人数：
    public static final String KEY_TRAVEL_MEMBER_ADULT = "travel_member_adult";

    //人员构成_儿童人数：
    public static final String KEY_TRAVEL_MEMBER_YONGER = "travel_member_yonger";

    //人员构成_婴儿人数：
    public static final String KEY_TRAVEL_MEMBER_BABY = "travel_member_baby";

    //产品价格：
    public static final String KEY_PRODUCT_PRICE = "product_price";

    //产品费用：
    public static final String KEY_PRODUCT_FEE = "product_fee";

    //活动特价：0无活动特价|其他为折扣率如0.8
    public static final String KEY_HAS_ACTIVITY = "has_activity";

    //下单时间：
    public static final String KEY_ORDER_CT = "order_ct";


    //====================================================
    //地区
    public static final String REGION_KEY_CODE = "region_code";
    public static final String REGION_KEY_CODE_DESC = "region_code_desc";
    public static final String REGION_KEY_CITY = "region_city";
    public static final String REGION_KEY_CITY_DESC = "region_city_desc";
    public static final String REGION_KEY_PROVINCE = "region_province";
    public static final String REGION_KEY_PROVINCE_DESC = "region_province_desc";

    //酒店
    public static final String PUB_KEY_ID = "pub_id";
    public static final String PUB_KEY_NAME = "pub_name";
    public static final String PUB_KEY_STAT = "pub_star";
    public static final String PUB_KEY_GRADE = "pub_grade";
    public static final String PUB_KEY_GRADEDESC = "pub_grade_desc";
    public static final String PUB_KEY_AREACODE = "pub_area_code";
    public static final String PUB_KEY_ADDRESS = "pub_address";
    public static final String PUB_KEY_IS_NATIONAL = "is_national";

    //产品
    public static final String PRODUCT_KEY_ID = "product_id";
    public static final String SHOP_KEY_ID = "shop_id";

    //=================================================================

    //地区信息
    public static List<RegionDO> regions = new ArrayList<RegionDO>();
    public static List<RegionDO> getRegions(){
        if(CollectionUtils.isEmpty(regions)){
            try {
                List<Map<String,String>> regionDatas= CSVUtil.readCSVFile(CSVUtil.REGION_FILE, CSVUtil.QUOTE_COMMON);
                if(CollectionUtils.isNotEmpty(regionDatas)){
                    for(Map<String,String> region : regionDatas){
                        RegionDO regionDO = new RegionDO();
                        regionDO.setRegionCity(region.getOrDefault(REGION_KEY_CITY,""));
                        regionDO.setRegionCityDesc(region.getOrDefault(REGION_KEY_CITY_DESC,""));
                        regionDO.setRegionCode(region.getOrDefault(REGION_KEY_CODE,""));
                        regionDO.setRegionCodeDesc(region.getOrDefault(REGION_KEY_CODE_DESC,""));
                        regionDO.setRegionProvince(region.getOrDefault(REGION_KEY_PROVINCE,""));
                        regionDO.setRegionProvinceDesc(region.getOrDefault(REGION_KEY_PROVINCE_DESC,""));
                        regions.add(regionDO);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("TravelOrderHeler.region.error:", e);
            }
        }
        return regions;
    }

    //酒店住宿
    public static List<String> pubs = new ArrayList<String>();
    public static List<String> getPubs(){
        if(CollectionUtils.isEmpty(pubs)){
            try {
                List<Map<String,String>> pubDatas= CSVUtil.readCSVFile(CSVUtil.PUB_FILE,CSVUtil.QUOTE_COMMON);
                if(CollectionUtils.isNotEmpty(pubDatas)){
                    for(Map<String,String> pub : pubDatas){
                        String pubID = pub.getOrDefault(PUB_KEY_ID,"");
                        pubs.add(pubID);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("TravelOrderHeler.pub.error:", e);
            }
        }
        return pubs;
    }

    /**
     * 旅游产品与酒店映射
     */
    public static Map<String,String> proMappingPub = new HashMap<String,String>();
    public static Map<String,String> getPubMappingPro(){
        if(MapUtils.isEmpty(proMappingPub)){
            try {
                List<Map<String,String>> pubDatas= CSVUtil.readCSVFile(CSVUtil.PUB_FILE,CSVUtil.QUOTE_COMMON);
                if(CollectionUtils.isNotEmpty(pubDatas)){
                    for(Map<String,String> pub : pubDatas){
                        String pubID = pub.getOrDefault(PUB_KEY_ID,"");
                        String[] pps = pubID.split("\\|");
                        proMappingPub.put(pps[0], pubID);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("TravelOrderHeler.pub2pro.error:", e);
            }
        }
        return proMappingPub;
    }


    //旅游产品
    public static List<String> product = new ArrayList<String>();
    public static List<String> getProducts(){
        if(CollectionUtils.isEmpty(product)){
            try {
                List<Map<String,String>> pubDatas= CSVUtil.readCSVFile(CSVUtil.PRODUCT_FILE,CSVUtil.QUOTE_COMMON);
                if(CollectionUtils.isNotEmpty(pubDatas)){
                    for(Map<String,String> pub : pubDatas){
                        String productID = pub.getOrDefault(PRODUCT_KEY_ID,"");

                        //product_id,product_title,product_level,product_type,product_type_desc,travel_day,travel_line_subjects,
                        // travel_lodging_info,product_price,departure,departure_code,first_arriving,first_arriving_code,
                        // des_out,des_city,des_city_desc,toursim_tickets_type,product_hot,shop_id
                        product.add(productID);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("TravelOrderHeler.product.error:", e);
            }
        }
        return product;
    }

    /**
     * 模拟旅游产品订单
     * @return
     */
    public static Map<String,Object> getTravelOrderData(String curTime, int countLevel, List<RegionDO> regions, List<String> productIDs, Map<String,String> pubs) throws Exception{
        Map<String,Object> result = new HashMap<String,Object>();

        //用户ID
        String userId = CommonUtil.getRandomNumStr(countLevel);
        result.put(KEY_USER_ID, userId);

        //预留手机号
        String userMobile = CommonUtil.getMobile();
        result.put(KEY_USER_MOBILE, userMobile);

        //旅游产品编号：
        String productID = CommonUtil.getRandomElementRange(productIDs);
        result.put(KEY_PRODUCT_ID, productID);

        //旅游产品住宿资源：
        String pubID = pubs.getOrDefault(productID,"");
        result.put(KEY_PRODUCT_PUB, pubID);

        //用户所在地区
        RegionDO regionDO = CommonUtil.getRandomElementRange(regions);
        String userRegion = regionDO.getRegionCity();
        result.put(KEY_USER_REGION, userRegion);

        //旅游产品交通资源：旅游交通选择:飞机|高铁|火车 (出行方式+座席)
        List<String> traffics = TrafficEnum.getGoodTraffics();
        String traffic = CommonUtil.getRandomElementRange(traffics);
        result.put(KEY_PRODUCT_TRAFFIC, traffic);

        //席座
        List<String> trafficSeas = new ArrayList<String>();
        if(TrafficEnum.AIRPLAN.getCode().equalsIgnoreCase(traffic)){
            trafficSeas = TrafficSeatEnum.getAirTrafficSeats();
        }else {
            trafficSeas = TrafficSeatEnum.getTrainTrafficSeats();
        }
        String trafficSea = CommonUtil.getRandomElementRange(trafficSeas);
        result.put(KEY_PRODUCT_TRAFFIC_GRADE, trafficSea);


        //单程|往返
        List<String> trafficTrips = TrafficGoBackEnum.getAllTrafficGoBacks();
        String trafficTrip = CommonUtil.getRandomElementRange(trafficTrips);
        result.put(KEY_PRODUCT_TRAFFIC_TYPE, trafficTrip);


        //人员构成_成人人数：
        int begin = CommonConstant.DEF_NUMBER_ONE;
        int end = CommonConstant.DEF_NUMBER_DUL;
        int sep = CommonConstant.DEF_NUMBER_ONE;
        List<Integer> adults = CommonUtil.getRangeNumeric(begin, end, sep);
        Integer adult = CommonUtil.getRandomElementRange(adults);
        result.put(KEY_TRAVEL_MEMBER_ADULT, String.valueOf(adult));

        //儿童及婴儿数量
        int yonger = 0;
        int baby = 0;
        if(CommonConstant.DEF_NUMBER_DUL == adult){
            int randomNumber = CommonUtil.getRandomNum(2)%3;
            if(randomNumber == 1){
                yonger = 1;
            }else if(randomNumber == 2){
                baby = 1;
            }
        }

        //人员构成_儿童人数：
        result.put(KEY_TRAVEL_MEMBER_YONGER, String.valueOf(yonger));

        //人员构成_婴儿人数：
        result.put(KEY_TRAVEL_MEMBER_BABY, String.valueOf(baby));


        //产品价格：
        List<Integer> prices = CommonUtil.getRangeNumeric(1, 10, 1);
        Integer price = CommonUtil.getRandomElementRange(prices);
        result.put(KEY_PRODUCT_PRICE, String.valueOf(price));


        //活动特价：0无活动特价|其他为折扣率如0.8
        int feeRatioRandom = CommonUtil.getRandomNum(1);
        int feeRatio = CommonConstant.DEF_NUMBER_ZERO;
        if(feeRatioRandom > 5){
            feeRatio = feeRatioRandom;
        }
        result.put(KEY_HAS_ACTIVITY, String.valueOf(feeRatio));


        //产品费用：
        int adultFee = price * adult;
        int yongerFee = price * yonger;
        int babyFee = price * baby;
        Integer totalFee = adultFee + yongerFee + babyFee;
        Double totalRealFeeTmp = Double.valueOf(totalFee);
        if(CommonConstant.DEF_NUMBER_ZERO != feeRatio){
            totalRealFeeTmp = totalFee * feeRatio / 10d;
        }
        Integer totalRealFee = totalRealFeeTmp.intValue();
        result.put(KEY_PRODUCT_FEE, String.valueOf(totalRealFee));


        //创建时间
        long ct = CommonUtil.getSelectTimestamp(curTime, CommonConstant.FORMATTER_YYYYMMDDHHMMDD);
        result.put(KEY_ORDER_CT, String.valueOf(ct));

        //订单编号ID: 时间戳+产品ID+用户ID
        String orderID = ct+productID+userId;
        result.put(KEY_ORDER_ID, orderID);

        //hashMD5
        String kafkaKey = CommonUtil.getMD5AsHex(orderID.getBytes());
        result.put(KEY_KAFKA_ID, kafkaKey);

        return result;
    }


    /**
     * 模拟旅游产品订单
     * @return
     */
    public static Map<String,Object> getTravelOrderDatas(String dts, int count, List<RegionDO> regions,
                                                         Map<String,String> pubs, List<String> productIDs) throws Exception{
        Map<String,Object> result = new HashMap<String,Object>();

        for(int i=1; i<=count; i++){
            Map<String,Object> subDatas = getTravelOrderData(dts, CommonConstant.USER_COUNT_LEVEL, regions, productIDs, pubs);
            result.putAll(subDatas);
        }
        return result;
    }


    /**
     * 测试原始数据
     * @param topic
     * @param count
     * @param beginDay
     * @param endDay
     * @param dayBegin
     * @param dayEnd
     * @param sleep
     * @throws Exception
     */
    public static void testTravelProductData(String topic, int count, String beginDay, String endDay, String dayBegin,String dayEnd,long sleep) throws Exception{
        //发送序列化对象
        String dateFormatter = CommonConstant.FORMATTER_YYYYMMDDHHMMDD;
        String dayFormatter = CommonConstant.FORMATTER_YYYYMMDD;
        ChronoUnit chronoUnit = ChronoUnit.MINUTES;
        ChronoUnit dayChronoUnit = ChronoUnit.DAYS;

        //辅助数据
        //地区信息
        List<RegionDO> regions = getRegions();
        Map<String,String> pubs = getPubMappingPro();
        List<String> productIDs = getProducts();

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

                    Map<String,Object> data = getTravelOrderDatas(curTime, count, regions, pubs, productIDs);
                    String datas = JSON.toJSONString(data);
                    System.out.println("ods.data send =" + datas);

                    String kafkaKey = data.getOrDefault(KEY_KAFKA_ID,"").toString();
                    String dataJson = JSON.toJSONString(data);
                    totalDatas.put(kafkaKey, dataJson);
                }
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
     * 测试原始数据
     * @param topic
     * @param count 周期产生数据
     * @param sleep
     * @throws Exception
     */
    public static void testTravelProductDataForevor(String topic, int count, long sleep) throws Exception{
        //发送序列化对象
        String dateFormatter = CommonConstant.FORMATTER_YYYYMMDDHHMMDD;
        String dayFormatter = CommonConstant.FORMATTER_YYYYMMDD;
        ChronoUnit chronoUnit = ChronoUnit.MINUTES;
        ChronoUnit dayChronoUnit = ChronoUnit.DAYS;

        //辅助数据
        //地区信息
        List<RegionDO> regions = getRegions();
        Map<String,String> pubs = getPubMappingPro();
        List<String> productIDs = getProducts();

        /**
         * 实时产生数据
         * 时间(天)范围轨迹数据
         */

        while(true){
            String curTime = CommonUtil.formatDate(new Date(), dateFormatter);
            Map<String,String> totalDatas = new HashMap<String,String>();

            for(int y=1; y<count; y++){
                Map<String,Object> data = getTravelOrderDatas(curTime, count, regions, pubs, productIDs);
                String datas = JSON.toJSONString(data);
                System.out.println("ods.data send =" + datas);

                String kafkaKey = data.getOrDefault(KEY_KAFKA_ID,"").toString();
                String dataJson = JSON.toJSONString(data);
                totalDatas.put(kafkaKey, dataJson);
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

        Integer count = tools.getInt(KEY_COUNT);
        Integer sleep = tools.getInt(KEY_SLEEP);


        testTravelProductDataForevor(topic, count, sleep);

//        if(SOURCE_PRODUCT.equalsIgnoreCase(source)){
//            testTravelProductData(topic, count, beginDay, endDay, dayBegin, dayEnd, sleep);
//        }else if(SOURCE_PAY.equalsIgnoreCase(source)){
//
//        }


    }

    //=====参数==================================================================

    public static final String KEY_TOPIC = "topic";
    public static final String KEY_SOURCE = "source";
    public static final String KEY_BEGIN = "begin";
    public static final String KEY_END = "end";
    public static final String KEY_COUNT = "count";
    public static final String KEY_SLEEP = "sleep";

    public static final String SOURCE_PRODUCT = "product";
    public static final String SOURCE_PAY = "pay";

    public static final String TIME_ORDER = "time_order";

    public static final Integer TIME_RANGE_MIN = 1;
    public static final Integer TIME_RANGE_MAX = 120;

    public static final Integer COUNT_MIN = 1;
    public static final Integer COUNT_MAX = 10000;

    public static final Integer SLEEP_MIN = 1000;
    public static final Integer SLEEP_MAX = 3600 * 1000;



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
        if(!SOURCE_PRODUCT.equalsIgnoreCase(source) && !SOURCE_PAY.equalsIgnoreCase(source)){
            source = "source is error['product','pay']!";
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
     * 示例： pragram argument 配置为： --topic t_travel_ods --source product -begin 20191212103000 --end 20191212103500 --count 1000 --sleep 3000
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
        //运行示例
//        String s = "--topic topic-dll --source product --begin 20200103153500 --end 20200103153510 --count 2 --sleep 1000";
//        String[] args2 = s.split(" ");

        String checkResult = checkParams(args);
        if(StringUtils.isEmpty(checkResult)){
            chooseFun(args);
        }else{
            System.out.println(checkResult);
        }
    }
}
