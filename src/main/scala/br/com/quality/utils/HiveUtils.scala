package br.com.quality.utils

import org.apache.spark.sql.SparkSession


object HiveUtils {

		private val getListOfColunsString =
			s"""
				 | SHOW COLUMNS FROM {{table_name}}
			""".stripMargin

		private val sumOfNullsNotNulls =
			s"""
				 | SELECT SUM(a.soma) FROM (
				 | SELECT ({{select_columns}}) as soma
				 |   FROM {{table_name}}
				 |  WHERE 1 = 1
				 |  {{where_filter}}
				 |  ) a
			""".stripMargin

		private val tempDropAmostragemTez =
			s"""
				 |   drop table if exists desenv.qlto_amostras_tez
				 |    """.stripMargin

		private val tempAmostragemTez =
			s"""
				 |   create table desenv.qlto_amostras_tez as
				 |   {{sql}}
				 |
				|    """.stripMargin

		private val sumOfColunsCaracterEspecial =
			s"""
				 | SELECT count(1) as qt_crtr_espl
				 |   FROM {{table_name}}
				 |  WHERE {{where_filter}}
			""".stripMargin

		def getListOfColuns(spark: SparkSession, tabela: String) : String = {
			val df1 = spark.sql(getListOfColunsString.replace("{{table_name}}", tabela))
			val list = df1.collect().map(_(0)).toList
			var stringRet = ""
			list.foreach(stringRet += _+",")
			stringRet.substring(0, stringRet.length-1)
		}

		def getCountCaracterEspecial(spark: SparkSession, tabela: String) : Long = {
			val df1 = spark.sql(getListOfColunsString.replace("{{table_name}}", tabela))
			val list = df1.collect().map(_(0)).toList
			var stringRet = ""
			list.foreach(stringRet += _+" RLIKE '[^a-zA-Z0-9\\\\d\\\\s:]' OR ")
			val where = stringRet.substring(0, stringRet.length-3)

			val query = sumOfColunsCaracterEspecial.replace("{{table_name}}",tabela).replace("{{where_filter}}", where)

			val df2 = spark.sql(query)
			df2.collectAsList().get(0).getAs[Long](0)
		}

		def getListOfColunsCastString(spark: SparkSession, tabela: String) : String = {
				val df1 = spark.sql(getListOfColunsString.replace("{{table_name}}", tabela))
				val list = df1.collect().map(_(0)).toList
				var stringRet = ""
				list.foreach(stringRet += "cast("+_+" as String),")
				stringRet.substring(0, stringRet.length-1)
		}

		def getSqlSumOfNullsNotNulls(spark: SparkSession, sumofnulls: Boolean, tabela: String, where: String="", execSpark: Boolean= true) : String = {
				val df1 = spark.sql(getListOfColunsString.replace("{{table_name}}", tabela))
				val list = df1.collect().map(_(0)).toList
				var stringRet = ""
				val selectColumnsPart = if(sumofnulls) " 1 ELSE 0 END)+" else " 0 ELSE 1 END)+"
				for (col <- list){
					stringRet += "(CASE WHEN ("+col+" IS NULL or "+col+" = 'null') THEN "+selectColumnsPart
				}
				stringRet = stringRet.substring(0, stringRet.length-1)
				sumOfNullsNotNulls.replace("{{table_name}}", tabela).replace("{{select_columns}}", stringRet).replace("{{where_filter}}", if (where.equals("")) "" else "and " + where)
		}

		def getSumOfNullsNotNulls(spark: SparkSession, sumofnulls: Boolean, tabela: String, where: String="", execSpark: Boolean= true) : Long = {
				val sql = getSqlSumOfNullsNotNulls(spark, sumofnulls, tabela, where, execSpark)
				if (execSpark) {
						val df1 = spark.sql(sql)
						df1.collectAsList().get(0).getAs[Long](0)
				} else {
						spark.sql(tempDropAmostragemTez)
						beelineExec(tempAmostragemTez.replace("{{sql}}", sql))
						val df2 = spark.sql("select * from desenv.qlto_amostras_tez")
						df2.collectAsList().get(0).getAs[Long](0)
				}
		}

		def beelineExec(hql: String, queue: String = "Qualidade"): String = {

			val serversHive=List(
				"server.host.br:2181",
				"server.host.br:2182")

			val bl =
				Seq("beeline",
					"-u",
					"'jdbc:hive2://"+gerarListaRandom(serversHive).mkString(",")+"/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2'",
					"--hiveconf",
					"tez.queue.name=" + queue,
					"--hiveconf",
					"mapreduce.job.queuename=" + queue,
					"--hiveconf",
					"hive.cbo.enable=true",
					"--hiveconf",
					"hive.stats.fetch.column.stats=true",
					"--hiveconf",
					"hive.stats.fetch.partition.stats=true",
					"--hiveconf",
					"hive.stats.autogather=true",
					"--hiveconf",
					"hive.exec.dynamic.partition.mode=nonstrict",
					"--hiveconf",
					"hive.exec.max.dynamic.partitions=100000",
					"--hiveconf",
					"hive.exec.max.dynamic.partitions.pernode=100000",
					"--hiveconf",
					"hive.merge.size.per.task=256000000",
					"--hiveconf",
					"hive.merge.mapfiles=true",
					"--hiveconf",
					"hive.merge.mapredfiles=true",
					"--hiveconf",
					"hive.merge.smallfiles.avgsize=128000000",
					"--hiveconf",
					"hive.merge.tezfiles=true",
					"--hiveconf",
					"hive.merge.orcfile.stripe.level=true",
					"--showheader=false",
					"--outputformat=tsv2",
					"-e",
					"\""+hql+"\""
				)

			import sys.process._
			bl !!
		}

		def gerarListaRandom(as: List[String]) = {
				val n = as.size
				scala.util.Random.shuffle(as).take(n)
		}

}
