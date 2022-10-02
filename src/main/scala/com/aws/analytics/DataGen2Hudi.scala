package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.sql.HudiTableSql
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.logging.log4j.LogManager
//import org.apache.logging.log4j.LogManager
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.EnvironmentSettings


import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
  object DataGen2Hudi{
    private val log = LogManager.getLogger(DataGen2Hudi.getClass)
    def main(args: Array[String]) {
      val params = Config.parseConfig(DataGen2Hudi, args)
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.enableCheckpointing(params.checkpointInterval.toInt * 1000)
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
      env.getCheckpointConfig.setCheckpointTimeout(60000)
      env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
      val rocksBackend: StateBackend = new RocksDBStateBackend(params.checkpointDir)
      env.setStateBackend(rocksBackend)

      val settings = EnvironmentSettings.newInstance.useBlinkPlanner().inStreamingMode().build()
      val tEnv = StreamTableEnvironment.create(env, settings)
      val conf = new Configuration()
      conf.setString("pipeline.name","datagen to hudi");
      tEnv.getConfig.addConfiguration(conf)

      val datagenSQl =
        s"""
           |CREATE TABLE datagen_tb (
           |    id string,
           |    name string,
           |    create_time   TIMESTAMP(3),
           |    modify_time   TIMESTAMP(3)
           |) WITH (
           |  'connector' = 'datagen',
           |  'rows-per-second' = '${params.rowsPerSecond}'
           |)
           |""".stripMargin

      tEnv.executeSql(datagenSQl)

      tEnv.executeSql(HudiTableSql.createHudiTB(params.hudiPath,params.hudiTableName))

      val stat =  tEnv.createStatementSet()
      stat.addInsertSql(HudiTableSql.insertTBSQL(params.hudiTableName,"datagen_tb"))
      stat.execute()
    }
  }