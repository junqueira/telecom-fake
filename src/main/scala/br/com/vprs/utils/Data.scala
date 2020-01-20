package br.com.vrps.utils

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SparkSession


object Data {

		def stringToDate(dtFotoRef:String = dateToString()): Date = {
				val simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
				val calendar:Calendar = Calendar.getInstance()
				val date:Date = simpleDateFormat.parse(dtFotoRef)
				calendar.setTime(date)
				date
		}

		def addDia(date: Date= Calendar.getInstance().getTime, amount: String = "-1"): Date ={
				val cal = Calendar.getInstance()
				cal.setTime(date)
				cal.add(Calendar.DATE, amount.toInt)
				cal.getTime
		}

		def dateToString(date: Date= Calendar.getInstance().getTime, format: String = "yyyyMMdd"): String = {
				val simpleDateFormat:SimpleDateFormat = new SimpleDateFormat(format)
				simpleDateFormat.format(date)
		}

		def getCodigoSemana(spark: SparkSession, dtFoto: String, dtFotoFormat: String="yyyyMMdd"): String ={
				spark.sql("select cast(from_unixtime(unix_timestamp('"+dtFoto+"','"+dtFotoFormat+"'), 'u') as int) as cd_smna")
					.collectAsList().get(0).getAs[Int](0).toString
		}

		def getTimestamp(): String ={
				System.currentTimeMillis().toString
		}
}
