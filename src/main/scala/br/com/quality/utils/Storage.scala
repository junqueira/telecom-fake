package br.com.quality.utils

import jp.co.bizreach.s3scala.S3
import jp.co.bizreach.s3scala.IOUtils
import awscala.s3._
import awscala.Region
import java.io.File

import com.amazonaws.services.s3.model.{GetObjectRequest, PutObjectRequest}
//              import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.io.Source
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SaveMode}
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory


object Storage {

		val conf = ConfigFactory.load()

		// required configuration values
		val awsAccessKey = conf.getString("main.awsAccessKey")
		val awsSecretKey = conf.getString("main.awsSecretKey")
		val s3ParquetPath = conf.getString("main.s3ParquetPath")
		val showCount = conf.getInt("main.showCount")

		implicit val region = Region.Tokyo
		implicit val s3 = S3(accessKeyId = awsAccessKey, secretAccessKey = awsSecretKey)

		def test(): Unit = {
				val ss = SparkSession.builder
					.master("local")
					.appName("read-parquet-s3")
					.getOrCreate()

				// set up Hadoop configuration
				val sc = ss.sparkContext
				val hadoopConfig = sc.hadoopConfiguration
				hadoopConfig.set("fs.s3a.access.key", awsAccessKey)
				hadoopConfig.set("fs.s3a.secret.key", awsSecretKey)

				// read the parquet from S3
				val dataset = ss.read.parquet(s3ParquetPath)
				dataset.show(showCount)
		}

		def sendBaseS3(bucketName: String, fileName: String): Unit = {
				val bucket: Bucket = s3.createBucket(bucketName)
				bucket.put(fileName, new java.io.File(fileName))
		}

		def getBaseS3(bucketName: String, fileName: String): String = {
				val objectFile = s3.getObject(new GetObjectRequest(bucketName, fileName))
				val file = Source.fromInputStream(objectFile.getObjectContent)
				file.getLines().mkString
		}

}
