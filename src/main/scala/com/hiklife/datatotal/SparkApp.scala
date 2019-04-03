package com.hiklife.datatotal

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{Callable, Executors}

import com.hiklife.utils._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkApp {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val configUtil = new ConfigUtil(path + "dataAnalysis/dataTotal.xml")
    var hBaseUtil = new HBaseUtil(path + "hbase/hbase-site.xml")
    var logUtil = new LogUtil(path + "log4j/log4j.properties")

    val conf = new SparkConf().setAppName(configUtil.appName)
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(configUtil.duration))

    val executors = Executors.newCachedThreadPool()
    executors.submit(new Callable[Unit] {
      override def call(): Unit ={
        //广播变量传递参数，减少副本数
        val broadList = sc.broadcast(List(configUtil.redisHost,configUtil.redisPort,configUtil.redisTimeout,configUtil.totalKey,
          configUtil.devTotalKey,configUtil.tagKey,configUtil.tagType,configUtil.kafkaOffsetKey))
        //从redis中获取kafka上次消费偏移量
        val redisUtil = new RedisUtil(configUtil.redisHost,configUtil.redisPort,configUtil.redisTimeout)
        val results = redisUtil.getObject(configUtil.kafkaOffsetKey)
        //创建sparkstreaming
        val topic=createTopic(configUtil)
        val kafkaParam = createKafkaParam(configUtil)
        val stream = if (results == null || results == None){
          //从最新开始消费
          KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent,Subscribe[String,String](topic,kafkaParam))
        }else{
          //从上次消费偏移位置开始消费
          var fromOffsets: Map[TopicPartition, Long] = Map()
          val map = results.asInstanceOf[Map[String,String]]
          for (result <- map) {
            val nor = result._1.split("_")
            val tp = new TopicPartition(nor(0), nor(1).toInt)
            fromOffsets += (tp -> result._2.toLong)
          }
          KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent,Subscribe[String,String](topic,kafkaParam,fromOffsets))
        }

        //epc 电池点量 场强 采集时间 设备编号 经度 纬度 进出标志 辅助时间
        stream.map(x => {
          val arr = x.value().split("\t")
          if(arr(7).equals("1")) //只计算进入数据
          {
            val redisUtil = new RedisUtil(broadList.value(0).toString, broadList.value(1).toString.toInt, broadList.value(2).toString.toInt)
            val tagKey = broadList.value(5).toString
            val ty = broadList.value(6).toString
            val tagType = redisUtil.getValue(tagKey+"_"+arr(0))
            if (tagType != null && tagType != None && tagType != "" && tagType.equals(ty)) {
              (arr(4) + "," + arr(3), 1) //设备编号+采集时间
            }else{
              if (ty.equals("1")){ //不存在，进电动车
                (arr(4) + "," + arr(3), 1) //设备编号+采集时间
              }else {
                ("error", 0)
              }
            }
          }else {
            ("error", 0)
          }
        }).reduceByKey(_ + _)
          .foreachRDD(rdd =>{
            rdd.foreachPartition(partitionOfRecords => {
              val redisUtil = new RedisUtil(broadList.value(0).toString, broadList.value(1).toString.toInt, broadList.value(2).toString.toInt)
              val totalKey = broadList.value(3).toString
              val devTotalKey = broadList.value(4).toString
              partitionOfRecords.foreach(record => {
                if(record._1 != "error") {
                  val records = record._1.split(",")
                  val sDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                  val dt = sDateFormat.parse(records(1))

                  //总数
                  redisUtil.incrBy(totalKey, record._2)

                  //月
                  val formatter_month = new SimpleDateFormat("yyyyMM")
                  val dt_month = formatter_month.format(dt)
                  redisUtil.incrBy(totalKey+"_"+dt_month, record._2)

                  //日
                  val formatter_day = new SimpleDateFormat("yyyyMMdd")
                  val dt_day = formatter_day.format(dt)
                  redisUtil.incrBy(totalKey+"_"+dt_day, record._2)

                  //设备月
                  redisUtil.incrBy(devTotalKey+"_"+dt_month+"_"+records(0), record._2)

                  //设备日
                  redisUtil.incrBy(devTotalKey+"_"+dt_day+"_"+records(0), record._2)

                  //设备小时
                  val formatter_hour = new SimpleDateFormat("yyyyMMddHH")
                  val dt_hour = formatter_hour.format(dt)
                  redisUtil.incrBy(devTotalKey+"_"+dt_hour+"_"+records(0), record._2)

                  //设备分
                  val formatter_minute = new SimpleDateFormat("yyyyMMddHHmm")
                  val dt_minute = formatter_minute.format(dt)
                  redisUtil.incrBy(devTotalKey+"_"+dt_minute+"_"+records(0), record._2)
                }
              })
            })

          })

        //记录本次消费offset
        stream.foreachRDD(rdd =>{
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd.foreachPartition(partitionOfRecords => {
            val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
            val key = s"${o.topic}_${o.partition}"
            val redisUtil = new RedisUtil(broadList.value(0).toString, broadList.value(1).toString.toInt, broadList.value(2).toString.toInt)
            val kafkaOffsetKey = broadList.value(7).toString

            var isRun = false
            while (!isRun){
              isRun = setOffset(redisUtil,kafkaOffsetKey,key,o.fromOffset.toString)
            }

          })
        })

        ssc.start()
        ssc.awaitTermination()

      }
    })

    val hourTotal = new HourTotal(hBaseUtil,configUtil,logUtil)
    hourTotal.start
  }

  /**
    * 创建kafka参数
    *
    * @param configUtil
    * @return
    */
  def createKafkaParam(configUtil: ConfigUtil): collection.Map[String, Object] ={
    val kafkaParam = Map(
      "bootstrap.servers" -> configUtil.brokers,//kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> configUtil.group, //用于标识这个消费者属于哪个消费团体
      "auto.offset.reset" -> configUtil.offset,//latest自动重置偏移量为最新的偏移量
      "enable.auto.commit" -> (false: java.lang.Boolean)//如果是true，则这个消费者的偏移量会在后台自动提交
    )
    kafkaParam
  }

  /**
    * 创建kafka主题
    *
    * @param configUtil
    * @return
    */
  def createTopic(configUtil: ConfigUtil): Iterable[String] ={
    var topic=Array(configUtil.topic)
    topic
  }

  def setOffset(redisUtil: RedisUtil, kafkaOffsetKey: String, fromOffsetKey: String, fromOffsetVal: String): Boolean ={
    val results = redisUtil.getObject(kafkaOffsetKey)
    if (results == null || results == None) {
      val map = Map(fromOffsetKey -> fromOffsetVal)
      redisUtil.watchSetObject(kafkaOffsetKey, map)
    } else {
      var map = results.asInstanceOf[Map[String, String]]
      map += (fromOffsetKey -> fromOffsetVal)
      redisUtil.watchSetObject(kafkaOffsetKey, map)
    }
  }

}
