package com.hiklife.datatotal

import org.apache.commons.configuration.HierarchicalConfiguration.Node
import org.apache.commons.configuration.XMLConfiguration

class ConfigUtil(path: String) extends Serializable {
  val conf = new XMLConfiguration(path)

  def getConfigSetting(key: String, default: String): String ={
    if(conf != null)
      conf.getString(key)
    else
      default
  }

  /*
  spark应用名称
   */
  val appName: String = getConfigSetting("appName", "DataTotal")

  /*
  每隔多少时间从kafka获取数据，单位秒
   */
  val duration: Long = getConfigSetting("duration", "10").toLong

  /*
  kafka集群地址
   */
  val brokers: String = getConfigSetting("brokers", "")

  /*
  kafka消费团体
   */
  val group: String = getConfigSetting("group", "DataRecoderGroup")

  /*
  kafka订阅主题
   */
  val topic: String = getConfigSetting("topic", "TagTopic")

  /*
  kafka消费偏移量方式
   */
  val offset: String = getConfigSetting("offset", "latest")

  /*
  设备采集实时统计表(分)
   */
  val devMinuteTotalTable: String = getConfigSetting("devMinuteTotalTable", "")

  /*
  设备采集实时统计表(小时)
   */
  val devHourTotalTable: String = getConfigSetting("devHourTotalTable", "")

  /*
  设备采集实时统计表(日)
   */
  val devDayTotalTable: String = getConfigSetting("devDayTotalTable", "")

  /*
  设备采集实时统计表(月)
   */
  val devMonthTotalTable: String = getConfigSetting("devMonthTotalTable", "")

  /*
  redis host
   */
  val redisHost: String = getConfigSetting("redisHost", "")

  /*
  redis port
   */
  val redisPort: Int = getConfigSetting("redisPort", "").toInt

  /*
  redis timeout
   */
  val redisTimeout: Int = getConfigSetting("redisTimeout", "").toInt

  /*
  kafka redis key
   */
  val kafkaOffsetKey: String = getConfigSetting("kafkaOffsetKey", "")

  /*
  total redis key
   */
  val totalKey: String = getConfigSetting("totalKey", "")

  /*
  dev total redis key
   */
  val devTotalKey: String = getConfigSetting("devTotalKey", "")

  /*
  tag redis key
   */
  val tagKey: String = getConfigSetting("tagKey", "")

  /*
  tagtype
   */
  val tagType: String = getConfigSetting("tagType", "")

}
