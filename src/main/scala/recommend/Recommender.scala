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

  /**
    * Compute rating prediction using the baseline method.
    *
    * @param train RDD
    * @param test RDD
    * 
    * @return RDD[Rating] with the predicted rating for each (user, item) pair
    */
  def bonusPrediction(train : RDD[Rating], test : RDD[(Int, Int)]) : RDD[Rating] = {

    // Calculate global average rating
    val globalAvg = train.averageRating

    val userAverageRating = train.toUserPair.averageByKey
    
    val normalizedDeviations = train
      .map(r => (r.user, (r.item, r.rating)))
      .join(userAverageRating)
      .map { case (u, ((i, r), ua)) => Rating(u, i, 1.0 * (r - ua) / Predictor.scale(r, ua).toDouble) }    
    
    val itemGlobalAverageDeviation = normalizedDeviations.toItemPair.averageByKey

    val ratingsPerItem = train.toItemPair.countByKey()

    val (min, max) = (ratingsPerItem.values.min, ratingsPerItem.values.max)

    def popularityPenalty(noRatings : Long) = (0.2 / (1.0 + scala.math.exp(10.0 * ((noRatings - min) / (max - min).toDouble) - 1.0)))

    // Verify that normalized deviations are within range and distinct for (user, item) pairs
    // assert(normalizedDeviations.filter(r => (r.rating > 1.0) || (r.rating < -1.0)).count == 0, 
    //        "Normalization not within range.")
    // assert(normalizedDeviations.map(r => (r.user, r.item)).distinct.count == train.count,
    //        "Non unique pairs of (user, item).")

    val predictions = test
      .join(userAverageRating)
      .map { case (u, (i, ua)) => (i, (ua, u)) }
      .leftOuterJoin(itemGlobalAverageDeviation)
      .map { case (i, ((ua, u), ia)) =>
        ia match {
          case None => Rating(u, i, globalAvg)
          case Some(ia) => 
            Rating(u, i, (ua + ia * Predictor.scale((ua + ia), ua)) - popularityPenalty(ratingsPerItem(i)))
        }
      }

    // Verify that all predictions are in the range [1.0, 5.0]
    // assert(predictions.filter(p => (p.rating < 1.0) || (p.rating > 5.0)).count == 0,
    //        "Some predictions are out of bounds")

    return predictions

  } 

  /**
    * Recommend movies for a specific user using a specific predictor.
    *
    * @param ratings RDD of item ratings by users
    * @param userId
    * @param n top rated predictions to output
    * @param predictor function that uses train and test sets to predict ratings
    * 
    * @return list containing (item, rating) pairs for the user with ID userId
    */
  def recommend(
    ratings : RDD[Rating],
    userId : Int,
    n : Int,
    predictor : (RDD[Rating], RDD[(Int, Int)]) => RDD[Rating] = Predictor.baselinePrediction
  ) : List[(Int, Double)] = {
    
    val ratedItems = ratings.filter(_.user == userId).map(_.item).collect()
    
    // Create test set
    val test = ratings
      .map(_.item).distinct
      .filter(!ratedItems.contains(_))
      .map(i => (userId, i))
    
    val predictions = predictor(ratings, test).filter(_.user == userId)
    
    // Sort by item first to have ascending movie IDs for equally rated predictions
    return predictions
      .sortBy(_.item).sortBy(_.rating, false)
      .toItemPair
      .take(n).toList
    
  }
  
  // Add my personal item ratings to RDD of item ratings by users
  val updatedRatings = data.union(personalRatings)
  
  // Q4.1.1
  val recommendations = recommend(updatedRatings, 944, 5)

  // Q4.1.2
  val recommendationsBonus = recommend(updatedRatings, 944, 5, bonusPrediction)

  // Get movie names from `personal.csv` and store in hashmap
  val moviesFile = spark.sparkContext.textFile(conf.personal())
  val movies = moviesFile
    .map(_.split(",").map(_.trim))
    .map(cols => (cols(0).toInt, cols(1).toString))
    .collect.toMap

  // Add movie name to recommendations
  def pretty(recommendations : List[(Int, Double)]) : List[Any] = 
    recommendations
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
            "Q4.1.1" -> pretty(recommendations),
            "Q4.1.2" -> pretty(recommendationsBonus)
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
