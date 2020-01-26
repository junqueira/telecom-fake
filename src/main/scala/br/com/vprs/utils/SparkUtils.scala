package br.com.vprs.utils

import org.apache.spark.sql.SparkSession


object SparkUtils {

 // def getSparkSession(fonte: String, queue: String = "vrps"): SparkSession = {

      //SparkSession
      //    .builder()
      //    .appName(queue + "-" + fonte)
      //    .config("spark.yarn.queue", queue)
      //    .config("spark.sql.codegen.aggregate.map.twolevel.enable", "false")
      //    .config("spark.sql.caseSensitive", "false")
      //    .config("spark.shuffle.service.enabled", "true")
      //    .config("spark.dynamicAllocation.enabled", "true")
      //    .config("spark.dynamicAllocation.initialExecutors", "3")
      //    .config("spark.dynamicAllocation.minExecutors", "3")
      //    .config("spark.dynamicAllocation.maxExecutors", "256")
      //    .config("spark.executor.instances", "3")
      //    .config("spark.executor.cores", "16")
      //    .config("spark.executor.memory", "50G")
      //    .config("spark.yarn.executor.memoryOverhead", "50000")
      //    .config("spark.driver.memory", "90G")
      //    .config("spark.yarn.driver.memoryOverhead", "20000")
      //    .config("spark.scheduler.mode", "FIFO")
      //    .config("spark.ui.port", "4142")
      //    //.config("spark.shuffle.compress", "true")
      //    .config("spark.hadoop.yarn.resourcemanager.webapp.address", "server.host.br:8088")
      //    .config("spark.master", "yarn")
      //    .config("hive.execution.engine", "spark")
      //    .config("hive.merge.mapredfiles", "true")
      //    .config("hive.merge.size.per.task", "128000000")
      //    .config("hive.merge.smallfiles.avgsize", "128000000")
      //    .config("hive.auto.convert.join", "true")
      //    .config("hive.auto.convert.sortmerge.join", "true")
      //    .config("hive.exec.dynamic.partition", "true")
      //    .config("hive.exec.dynamic.partition.mode", "nonstrict")
      //    .config("hive.exec.max.dynamic.partitions", "500000")
      //    .config("hive.vectorized.execution.enabled", "true")
      //    .config("hive.vectorized.execution.reduce.enabled", "true")
      //    .config("hive.cbo.enable", "true")
      //    .config("hive.compute.query.using.stats", "true")
      //    .config("hive.stats.fetch.column.stats", "true")
      //    .config("hive.stats.fetch.partition.stats", "true")
      //    .config("tez.queue.name", queue)
      //    .config("mapreduce.job.queuename", queue)
      //    .enableHiveSupport()
      //    .getOrCreate()
  //}

def getSparkSession(fonte: String = "Desenvolvimento", queue : String="vprs", qt_exec_master: String = "*"): SparkSession = {
    SparkSession
      .builder()
      .appName(fonte)
      .config("spark.master", "yarn")
      .config("spark.yarn.queue", queue)
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.shuffle.compress", "true")
      .config("spark.shuffle.service.port","7337")
      .config("spark.executor.cores", "50")
      .config("spark.executor.memory", "100G")
      .config("spark.yarn.executor.memoryOverhead", "8000")
      .config("spark.driver.memory", "64G")
      .config("spark.yarn.driver.memoryOverhead", "8000")
      .config("spark.scheduler.mode", "FIFO")
      .config("spark.ui.port", "4066")
      .config("spark.driver.maxResultSize", "50G")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.dynamicAllocation.initialExecutors", "2")
      .config("spark.dynamicAllocation.minExecutors", "2")
      .config("spark.dynamicAllocation.maxExecutors", "52")
      .config("tez.queue.name", queue)
      .config("mapreduce.job.queuename", queue)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.broadcastTimeout", "36000")
      .enableHiveSupport()
      .getOrCreate()
  }
}