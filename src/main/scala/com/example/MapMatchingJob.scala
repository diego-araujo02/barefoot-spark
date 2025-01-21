import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.esri.core.geometry.Point
import com.bmwcarit.barefoot.matcher.{MatcherSample, MatcherKState}
import com.example.barefoot_spark.BroadcastMatcher
import org.json.JSONObject
import org.json.JSONArray
import java.time.format.DateTimeFormatter
import java.time.ZonedDateTime
import java.time.ZoneOffset
import scala.jdk.CollectionConverters._
import java.lang.Double

object MapMatchingJob {
  def main(args: Array[String]): Unit = {
    // Configurações do Spark
    val conf = new SparkConf().setAppName("Barefoot Map Matching with Hadoop").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // URI do mapa no HDFS
    val mapUri = "hdfs://localhost:9000/user/diego/barefoot_spark/oberbayern.bfmap"

    // BroadcastMatcher usando HadoopMapReader
    val matcher = sc.broadcast(new BroadcastMatcher(mapUri))

    // Carregar dados de rastreamento como RDD do arquivo JSON
    val traces = sc.textFile("src/main/scala/com/example/x0001-015.json").flatMap(line => {
      try {
        // Parse o conteúdo da linha como JSONArray
        val json = new JSONArray(line)

        // Itera sobre todos os objetos do JSONArray
        val processedData = for (i <- 0 until json.length()) yield {
          val jsonObject = json.getJSONObject(i)

          // Obtém o valor de "point" e faz o tratamento adequado
          val pointWKT = jsonObject.getString("point")
          val cleanedPoint = pointWKT.replace("POINT(", "").replace(")", "").trim
          val coordinates = cleanedPoint.split("\\s+")
          val point = new Point(Double.parseDouble(coordinates(0)), Double.parseDouble(coordinates(1)))

          // Obtém o id do objeto JSON
          val objectId = jsonObject.getString("id")

          // Processar o timestamp com o fuso horário
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssZ")
          val timestampStr = jsonObject.getString("time")
          val timestamp = ZonedDateTime.parse(timestampStr, formatter).toInstant.toEpochMilli
          println(objectId," / ", timestamp," / ",point)
          // Retorna os dados processados
          (objectId, timestamp, point)
        }

        // Retorna os dados processados como um único RDD
        processedData
      } catch {
        case e: Exception =>
          // Trata qualquer erro ocorrido e exibe a mensagem de erro
          println(s"Error processing line: $line, error: ${e.getMessage}")
          // Retorna valores padrão ou de erro
          Seq(("",0L, new Point(0, 0)))
      }
    })

    val matches = traces.groupBy(_._1).map { case (objectId, tripData) =>
      val tripSamples = tripData.toList.map { case (_, timestamp, point) =>
        new MatcherSample(timestamp, point)
      }.asJava

      val matchedState: MatcherKState = matcher.value.mmatch(tripSamples)
      (objectId, matchedState)
    }

    
    val outputPath = "src/main/scala/com/example/output/matched_results.json"
    matches.map { case (objectId, matchedState) =>
      try {
        val geoJson = matchedState.toGeoJSON().toString
        s"""{"objectId": "$objectId", "matchedState": $geoJson}"""
      } catch {
        case e: Exception =>
          s"""{"objectId": "$objectId", "error": "${e.getMessage}"}"""
      }
    }.saveAsTextFile(outputPath)

    sc.stop()
  }
}
