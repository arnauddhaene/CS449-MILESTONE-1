package stats

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
  //  MY CODE HERE
  // **************

  // Q3.1.1
  val globalAverageRating = data.map(rating => rating.rating).mean

  // Q3.1.{2,3}

  /**
    * Computes the average rating per item or user.
    *
    * @param data the RDD containing Ratings.
    * @param onKey the key to average on: one of {"user","item"}.
    * 
    * @return a RDD of (Int, Double) with the ID of {"user","item"} and the rating.
    */
  def averageRatingPer(data : RDD[Rating], onKey : String) : RDD[(Int, Double)] = {
    return data.map(r => (if(onKey == "item") r.item else r.user, r.rating))
      .aggregateByKey((0, 0))(
        (k, v) => (k._1 + v.toInt, k._2 + 1),
        (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .mapValues(sum => 1.0 * sum._1 / sum._2)
  }
  
  // TODO: documentation
  def ratioOfRatingsCloseToGlobalAverageRating(ratings: RDD[Double], globalAverage: Double, threshold: Double = 0.5) : Double = {
    return 1.0 * ratings.filter(r => (r - globalAverage).abs < threshold).count / ratings.count
  }

  // TODO: documentation
  def allRatingsCloseToGlobalAverageRating(ratings: RDD[Double], globalAverage: Double, threshold: Double = 0.5) : Boolean = {
    return (ratings.min > globalAverage - threshold) && (ratings.max < globalAverage + threshold)
  }
  

  val usersAverageRating = spark.time(averageRatingPer(data, "user").map(t => t._2))
  val itemsAverageRating = spark.time(averageRatingPer(data, "item").map(t => t._2))

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
                "min" -> usersAverageRating.min,  // Datatype of answer: Double
                "max" -> usersAverageRating.max, // Datatype of answer: Double
                "average" -> usersAverageRating.mean // Datatype of answer: Double
            ),
            "AllUsersCloseToGlobalAverageRating" -> allRatingsCloseToGlobalAverageRating(usersAverageRating, globalAverageRating), // Datatype of answer: Boolean
            "RatioUsersCloseToGlobalAverageRating" -> ratioOfRatingsCloseToGlobalAverageRating(usersAverageRating, globalAverageRating) // Datatype of answer: Double
          ),
          "Q3.1.3" -> Map(
            "ItemsAverageRating" -> Map(
                // Using as your input data the average rating for each item,
                // report the min, max and average of the input data.
                "min" -> itemsAverageRating.min,  // Datatype of answer: Double
                "max" -> itemsAverageRating.max, // Datatype of answer: Double
                "average" -> itemsAverageRating.mean // Datatype of answer: Double
            ),
            "AllItemsCloseToGlobalAverageRating" -> allRatingsCloseToGlobalAverageRating(itemsAverageRating, globalAverageRating), // Datatype of answer: Boolean
            "RatioItemsCloseToGlobalAverageRating" -> ratioOfRatingsCloseToGlobalAverageRating(itemsAverageRating, globalAverageRating) // Datatype of answer: Double
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
