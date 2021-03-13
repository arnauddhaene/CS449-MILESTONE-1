package recommend

import stats.Analyzer
import stats.Rating

import stats.RatingFunctions._
import stats.PairRDDFunctions._

import predict.Predictor

import predict.VectorFunctions._

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  val json = opt[String]()
  verify()
}

object Recommender extends App {
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

  // Q4.1.1

  println("Loading personal data from: " + conf.personal()) 
  val personalFile = spark.sparkContext.textFile(conf.personal())
  val personalRatings = personalFile
    .map(_.split(",").map(_.trim))
    .filter(_.length == 3)
    .map(cols => Rating(944, cols(0).toInt, cols(2).toDouble))

  val personalRatingsSet = personalRatings.map(_.rating)

  print(f"Personal ratings contain ${personalRatingsSet.count} ratings " +
    f"with an average of ${personalRatingsSet.mean}%1.2f, a min of " +
    f"${personalRatingsSet.min}%1.2f and max of ${personalRatingsSet.max}%1.2f.\n")

  assert(personalFile.count == 1682, "Invalid personal data")

  def recommend(ratings : RDD[Rating], userId : Int, n : Int) : List[(Int, Double)] = {
    
    val ratedItems = ratings.filter(_.user == userId).map(_.item).collect()
    
    // Create test set
    val test = ratings
      .map(_.item).distinct
      .filter(!ratedItems.contains(_))
      .map(i => (userId, i))
    
    val predictions = Predictor.baselinePrediction(ratings, test)
      .filter(_.user == userId)
    
    // Sort by item first to have ascending movie IDs for equally rated predictions
    return predictions
      .sortBy(_.item).sortBy(_.rating, false)
      .toItemPair
      .take(n).toList
    
  }
  
  val updatedRatings = data.union(personalRatings)
  
  val recommendations = recommend(updatedRatings, 944, 5)

  val moviesFile = spark.sparkContext.textFile(conf.personal())
  val movies = moviesFile
    .map(_.split(",").map(_.trim))
    .map(cols => (cols(0).toInt, cols(1).toString))
    .collect.toMap

  val prettyRecommendations = recommendations
    .map(mr => List(mr._1, movies(mr._1), mr._2))
    .toList

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

            // IMPORTANT: To break ties and ensure reproducibility of results,
            // please report the top-5 recommendations that have the smallest
            // movie identifier.
            "Q4.1.1" -> prettyRecommendations
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
