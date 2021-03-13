package stats

import stats.RatingFunctions._
import stats.PairRDDFunctions._

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

// Extension of RDD[Rating] with custom operators
class RatingFunctions(rdd : RDD[Rating]) {
  
  def averageRating = rdd.map(_.rating).mean

  def toUserItemPair = rdd.map(r => (r.user, r.item))
  def toUserPair = rdd.map(r => (r.user, r.rating))
  def toItemPair = rdd.map(r => (r.item, r.rating))

}

object RatingFunctions {
  implicit def addRatingFunctions(rdd: RDD[Rating]) = new RatingFunctions(rdd) 
}

// Extension of RDD[(Int, Double)] with custom operators
class PairRDDFunctions(rdd : RDD[(Int, Double)]) {

  def values = rdd.map(_._2)

  def averageByKey = rdd
    .aggregateByKey((0.0, 0))(
      (k, v) => (k._1 + v, k._2 + 1),
      (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
    .mapValues(sum => 1.0 * sum._1 / sum._2.toDouble)

  def ratioCloseTo(global : Double, threshold : Double = 0.5) = 
    1.0 * rdd.values.filter(r => (r - global).abs < threshold).count / rdd.values.count

  def allCloseTo(global: Double, threshold: Double = 0.5) =
    (rdd.values.min > global - threshold) && (rdd.values.max < global + threshold)

}

object PairRDDFunctions {
  implicit def addPairRDDFunctions(rdd: RDD[(Int, Double)]) = new PairRDDFunctions(rdd) 
}

object Analyzer extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new Conf(args) 
  println("Loading data from: " + conf.data()) 
  val dataFile = spark.sparkContext.textFile(conf.data())
  val data = dataFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(data.count == 100000, "Invalid data")

  // **************

  // Q3.1.1
  val globalAverageRating = data.averageRating

  // Q3.1.{2,3}  
  val usersAverageRating = data.toUserPair.averageByKey
  val itemsAverageRating = data.toItemPair.averageByKey

  // **************

  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q3.1.1" -> Map(
            "GlobalAverageRating" -> globalAverageRating // Datatype of answer: Double
          ),
          "Q3.1.2" -> Map(
            "UsersAverageRating" -> Map(
                // Using as your input data the average rating for each user,
                // report the min, max and average of the input data.
                "min" -> usersAverageRating.values.min,  // Datatype of answer: Double
                "max" -> usersAverageRating.values.max, // Datatype of answer: Double
                "average" -> usersAverageRating.values.mean // Datatype of answer: Double
            ),
            "AllUsersCloseToGlobalAverageRating" -> usersAverageRating.allCloseTo(globalAverageRating), // Datatype of answer: Boolean
            "RatioUsersCloseToGlobalAverageRating" -> usersAverageRating.ratioCloseTo(globalAverageRating) // Datatype of answer: Double
          ),
          "Q3.1.3" -> Map(
            "ItemsAverageRating" -> Map(
                // Using as your input data the average rating for each item,
                // report the min, max and average of the input data.
                "min" -> itemsAverageRating.values.min,  // Datatype of answer: Double
                "max" -> itemsAverageRating.values.max, // Datatype of answer: Double
                "average" -> itemsAverageRating.values.mean // Datatype of answer: Double
            ),
            "AllItemsCloseToGlobalAverageRating" -> itemsAverageRating.allCloseTo(globalAverageRating), // Datatype of answer: Boolean
            "RatioItemsCloseToGlobalAverageRating" -> itemsAverageRating.ratioCloseTo(globalAverageRating) // Datatype of answer: Double
          ),
         )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
