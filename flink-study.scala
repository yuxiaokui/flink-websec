import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON

import org.apache.flink.streaming.api.windowing.time.Time

import java.util.Properties
import java.util.ArrayList
import java.util.List

import scala.util.parsing.json._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink

import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests



object StreamingJob {

  def main(args: Array[String]): Unit = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val properties = new Properties()
      properties.setProperty("bootstrap.servers", "xxx")
      properties.setProperty("zookeeper.connnect", "xxx")
      properties.setProperty("group.id", "test-consumer-group")

      //read from socket
      val text = env.socketTextStream("localhost", 9999)
      
      env.enableCheckpointing(5000)
      
      //read from kfk
      val kafkaConsumer = new FlinkKafkaConsumer010(
          "metrics",
          new SimpleStringSchema,
          properties)

      //var text = env.addSource(kafkaConsumer)                
      
      //map this data
      val data = text.map { x => JSON.parseObject(x.toString) }

      val name = data.map { x => x.getString("__name__") }

      val value = data.map { x => x.getString("__value__") }

      //ES sink
      val httpHosts = new java.util.ArrayList[HttpHost]

      httpHosts.add(new HttpHost("10.1.48.7", 9200, "https"))
      
      val esSinkBuilder = new ElasticsearchSink.Builder[String] (
        httpHosts,
        new ElasticsearchSinkFunction[String] {
        def createIndexRequest(element: String) : IndexRequest  = {
          val json = new java.util.HashMap[String, String]
          
          json.put("data", element)

          return Requests.indexRequest()
                  .index("flink-demo")
                  .source(json)
          }
        }
      )
      

      name.addSink(esSinkBuilder.build)
  
      //data.writeAsText("/flink/name.log")
    
     /*
     val kafkaProducer_name = new FlinkKafkaProducer010(               
          "name",
          new SimpleStringSchema,
          properties)

     val kafkaProducer_value = new FlinkKafkaProducer010(
         "value",
         new SimpleStringSchema,
         properties)


      // write data into Kafka
  
      name.addSink(kafkaProducer_name)
      value.addSink(kafkaProducer_value)
      */


      env.execute("Kafka To ELK")
  }

  def tranTimeToString(tm:String): String = {
      val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val tim = fm.format(new Date(tm.toLong))
      tim
  }
}
