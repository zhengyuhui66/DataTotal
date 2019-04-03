package com.hiklife.datatotal

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.util.concurrent.{ExecutorService, Executors}

import com.hiklife.utils.{CommUtil, HBaseUtil, LogUtil, RedisUtil}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}

class HourTotal(hBaseUtil: HBaseUtil, configUtil: ConfigUtil, logUtil: LogUtil) {

  def start: Unit = {
    //创建表
    hBaseUtil.createTable(configUtil.devMinuteTotalTable, "CF")
    hBaseUtil.createTable(configUtil.devHourTotalTable, "CF")
    hBaseUtil.createTable(configUtil.devDayTotalTable, "CF")
    hBaseUtil.createTable(configUtil.devMonthTotalTable, "CF")

    //创建线程池
    val threadPool: ExecutorService = Executors.newFixedThreadPool(1)
    threadPool.execute(new synch())
  }

  class synch() extends Runnable {
    override def run() {
      while (true) {
        try {
          this.refresh
        }
        catch {
          case e: Exception =>
            logUtil.error("设备小时统计失败", e)
        }
        //休息30分钟
        for (i <- 0 to 1800) {
          Thread.sleep(1000)
        }
      }
    }

    def refresh: Unit = {
      val redisUtil = new RedisUtil(configUtil.redisHost, configUtil.redisPort, configUtil.redisTimeout)
      var calendar = Calendar.getInstance()
      calendar.add(Calendar.HOUR_OF_DAY, -1)
      val dt = calendar.getTime()

      val conn = ConnectionFactory.createConnection(hBaseUtil.conf)
      val table_minute = conn.getTable(TableName.valueOf(configUtil.devMinuteTotalTable)).asInstanceOf[HTable]
      val table_hour = conn.getTable(TableName.valueOf(configUtil.devHourTotalTable)).asInstanceOf[HTable]
      val table_day = conn.getTable(TableName.valueOf(configUtil.devDayTotalTable)).asInstanceOf[HTable]
      val table_month = conn.getTable(TableName.valueOf(configUtil.devMonthTotalTable)).asInstanceOf[HTable]

      //分
      putMinuteTable(dt, table_minute, redisUtil)
      //小时
      putHourTable(dt, table_hour, redisUtil)
      //日
      putDayTable(dt, table_day, redisUtil)
      //月
      putMonthTable(dt, table_month, redisUtil)

      conn.close()

      //删除昨天数据
      var del_calendar = Calendar.getInstance()
      del_calendar.add(Calendar.HOUR_OF_DAY, -1)
      val del_date = del_calendar.getTime()

      val formatter_hour = new SimpleDateFormat("yyyyMMddHH")
      val del_hour = formatter_hour.format(del_date)

      //日统计
      redisUtil.connect()
      val del_keys = redisUtil.jedis.keys(configUtil.devTotalKey + "_" + del_hour + "*").toArray()
      redisUtil.close()
      for (i <- 0 until del_keys.length) {
        val key = del_keys(i).toString
        val list = key.split("_")
        if (list(1).length() == 12) {
          redisUtil.del(key)
        }
      }
    }

    def putMinuteTable(dt: Date, table: HTable, redisUtil: RedisUtil): Unit = {
      table.setAutoFlush(false, false)
      table.setWriteBufferSize(5 * 1024 * 1024)

      val formatter_hour = new SimpleDateFormat("yyyyMMddHH")
      val formatter_day = new SimpleDateFormat("yyyyMMdd")
      val dt_hour = formatter_hour.format(dt)
      val dt_day = formatter_day.format(dt)

      redisUtil.connect()
      val keys = redisUtil.jedis.keys(configUtil.devTotalKey + "_" + dt_hour + "*").toArray()
      redisUtil.close()
      var map: Map[String, Int] = Map()
      for (i <- 0 until keys.length) {
        val key = keys(i).toString
        val value = redisUtil.getValue(key)
        val list = key.split("_")
        if (list(1).length() == 12) {
          val rowkey = Integer.toHexString(CommUtil.getHashCodeWithLimit(dt_day, 0xFFFE)).toUpperCase() + list(1)
          val put = new Put(rowkey.getBytes)
          put.addColumn("CF".getBytes, list(2).getBytes, value.toString.getBytes)
          table.put(put)
        }
      }

      table.flushCommits()
      table.close()
    }

    def putHourTable(dt: Date, table: HTable, redisUtil: RedisUtil): Unit = {
      table.setAutoFlush(false, false)
      table.setWriteBufferSize(5 * 1024 * 1024)

      val formatter_hour = new SimpleDateFormat("yyyyMMddHH")
      val formatter_day = new SimpleDateFormat("yyyyMMdd")
      val dt_hour = formatter_hour.format(dt)
      val dt_day = formatter_day.format(dt)

      redisUtil.connect()
      val keys = redisUtil.jedis.keys(configUtil.devTotalKey + "_" + dt_hour + "_*").toArray()
      redisUtil.close()
      var map: Map[String, Int] = Map()
      for (i <- 0 until keys.length) {
        val key = keys(i).toString
        val value = redisUtil.getValue(key)
        val list = key.split("_")
        val rowkey = Integer.toHexString(CommUtil.getHashCodeWithLimit(dt_day, 0xFFFE)).toUpperCase() + dt_hour
        val put = new Put(rowkey.getBytes)
        put.addColumn("CF".getBytes, list(2).getBytes, value.toString.getBytes)
        table.put(put)
      }

      table.flushCommits()
      table.close()
    }

    def putDayTable(dt: Date, table: HTable, redisUtil: RedisUtil): Unit = {
      table.setAutoFlush(false, false)
      table.setWriteBufferSize(5 * 1024 * 1024)

      val formatter_day = new SimpleDateFormat("yyyyMMdd")
      val formatter_month = new SimpleDateFormat("yyyyMM")
      val dt_day = formatter_day.format(dt)
      val dt_month = formatter_month.format(dt)

      redisUtil.connect()
      val keys = redisUtil.jedis.keys(configUtil.devTotalKey + "_" + dt_day + "_*").toArray()
      redisUtil.close()
      var map: Map[String, Int] = Map()
      for (i <- 0 until keys.length) {
        val key = keys(i).toString
        val value = redisUtil.getValue(key)
        val list = key.split("_")
        val rowkey = Integer.toHexString(CommUtil.getHashCodeWithLimit(dt_month, 0xFFFE)).toUpperCase() + dt_day
        val put = new Put(rowkey.getBytes)
        put.addColumn("CF".getBytes, list(2).getBytes, value.toString.getBytes)
        table.put(put)
      }

      table.flushCommits()
      table.close()
    }

    def putMonthTable(dt: Date, table: HTable, redisUtil: RedisUtil): Unit = {
      table.setAutoFlush(false, false)
      table.setWriteBufferSize(5 * 1024 * 1024)

      val formatter_month = new SimpleDateFormat("yyyyMM")
      val formatter_year = new SimpleDateFormat("yyyy")
      val dt_month = formatter_month.format(dt)
      val dt_year = formatter_year.format(dt)

      redisUtil.connect()
      val keys = redisUtil.jedis.keys(configUtil.devTotalKey + "_" + dt_month + "_*").toArray()
      redisUtil.close()
      var map: Map[String, Int] = Map()
      for (i <- 0 until keys.length) {
        val key = keys(i).toString
        val value = redisUtil.getValue(key)
        val list = key.split("_")
        val rowkey = Integer.toHexString(CommUtil.getHashCodeWithLimit(dt_year, 0xFFFE)).toUpperCase() + dt_month
        val put = new Put(rowkey.getBytes)
        put.addColumn("CF".getBytes, list(2).getBytes, value.toString.getBytes)
        table.put(put)
      }

      table.flushCommits()
      table.close()
    }

  }

}
