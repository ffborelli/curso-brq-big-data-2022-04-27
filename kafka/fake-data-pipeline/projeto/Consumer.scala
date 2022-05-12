import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

object Consumer {
  def main(args: Array[String]): Unit = {
    //import java.util.Properties
    //
    //// properties for jdbc
    //val properties = new Properties()
    //properties.put("user", "postgres")
    //properties.put("password", "**************")

    val spark = SparkSession
      .builder
      .master("yarn")
      .appName("Consumer")
      .getOrCreate()

    import spark.implicits._

    val inputDF = spark.readStream
      .format("kafka")
      .options(Map(
        "kafka.bootstrap.servers" -> "kafka-single-node:9092",
        "subscribe" -> "kafka-scala-topic")
      )
      .load()
    
    val rawDF = inputDF.selectExpr("topic", "CAST(value AS STRING)", "timestamp")
    //print(rawDF)
    val query = rawDF.writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .foreachBatch { (batchDF: DataFrame, _) =>
        // cache
        batchDF.persist()
        //println(batchDF.show())
        
        // Topic: iot
        batchDF.where($"topic" === "kafka-scala-topic")
          .withColumn("_tmp",$"value") 
          .agg(avg("_tmp"))
          .withColumnRenamed("avg(_tmp)", "temp")
          .show(3)
    
        println("Value from kafka")
        //println(batchDF.show())
        //println("write to postgresql")

        // uncache
        batchDF.unpersist()
      }
      .outputMode("update")
      .start()

    query.awaitTermination()
  }


}
