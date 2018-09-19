    package br.com.qualidade.monitor

    import br.com.qualidade.MonitorFunc
    import org.apache.log4j.Logger
    import org.apache.spark.sql.{DataFrame, SparkSession}

    import scala.util.{Failure, Success}
    import scala.collection.mutable.ListBuffer
    import java.util.concurrent.Executors
    import org.apache.spark.sql.{DataFrame, SparkSession, Row}

    import br.com.qualidade.utils.SparkUtils

    import scala.concurrent.duration._
    import scala.concurrent.{Await, ExecutionContext, Future}


    object MonitorBase {

        val log: Logger = Logger.getLogger(MonitorBase.getClass)

        def execute (spark: SparkSession): Unit = {
            log.info(s"Running Monitor - Mapping All Cluster Tables")
            val allTables = MonitorFunc.getAllTables(spark)
            var finalTables = new ListBuffer[List[Row]]()
            val qdtProcess = 1000
            val executorService  = Executors.newFixedThreadPool(qdtProcess)
            val executionContext = ExecutionContext.fromExecutorService(executorService)

            log.info(s"Start proccess concurrency" + " quantity => " + qdtProcess)
            def go(implicit ec: ExecutionContext): Future[Seq[List[DataFrame]]] = {
                val list =  Seq( Future {
                    allTables.map( table =>
                            Await.result(MonitorTable.execute( spark, table ), Duration.Inf)
                    )
                })
                Future.sequence(list)
            }
            var result = Await.result(go(executionContext), Duration.Inf)
            executionContext.shutdown()
            log.info(s"Incio processo unionAll df")
            var finalDF = result.map(df =>  df.reduce(_ union _))
            finalDF(0).show()
            saveMonitor(spark, finalDF(0))
        }

        def saveMonitor(spark: SparkSession, df: DataFrame): Unit = {
            val table = "p_desenvolvimento_db.tbgd_tdq_hstr_qlto_dia_tbla"
            df.write.mode("overwrite").insertInto(table)
        }
    }



