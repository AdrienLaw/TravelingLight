package com.adrien.bigdata.realtime.rdo

/**
 * 对旅游产品维度封装
 */
object QRealTimeDimDO {

  /**
   * 产品维度类封装
   * @param productID
   * @param productLevel
   * @param productType
   * @param depCode
   * @param desCode
   * @param toursimtype
   */
  case class ProductDimDO(productID:String,productLevel:Int,productType:String,
                          depCode:String,desCode:String,toursimtype:String)

  //酒店维度的封装---TODO
  case class PubDimDO()
}
