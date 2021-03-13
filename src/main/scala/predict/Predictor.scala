package predict

import stats.Analyzer
import stats.Rating

import stats.RatingFunctions._
import stats.PairRDDFunctions._

import predict.VectorFunctions._

import recommend.Recommender

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

// Extension of Vector[Double] with custom operators
class VectorFunctions(underlying : Vector[Double]) {
  
  def mean = underlying.sum / underlying.size.toDouble

  def stdev : Double = {
    val avg = underlying.sum / underlying.size.toDouble
    return scala.math.sqrt(
      underlying.map(t => scala.math.pow((t - avg), 2)).sum / underlying.size.toDouble
    )
  }

}

object VectorFunctions {
  implicit def addVectorFunctions(underlying : Vector[Double]) = new VectorFunctions(underlying) 
}

object FuncTimer {

  def time(func : => Any) : Double = {
    val start = System.nanoTime()
    val perform = func
    return (System.nanoTime() - start) / 1e6d
  }

}

object Predictor extends App {
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
  println("Loading training data from: " + conf.train()) 
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test()) 
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(test.count == 20000, "Invalid test data")
  
  // **************

  // Q3.1.4
  val globalAverageRating = train.averageRating
  val globalMae = test
    .map(r => scala.math.abs(r.rating - globalAverageRating))
    .mean
  
  /**
    * Computes the predictions on the test set using User Average.
    *
    * @param train the RDD containing the training Ratings
    * @param test the RDD containing the test (user, item) pairs
    * 
    * @return predictions
    */
  def predictionByUser(train : RDD[Rating], test : RDD[(Int, Int)]) : RDD[Rating] = {
    // Fetch average ratings
    val averageRatingByUser = train.toUserPair.averageByKey

    // Calculate global average rating
    val globalAverageRating = train.averageRating

    val predictions = test
      .leftOuterJoin(averageRatingByUser)
      .map { case (u, (i, ua)) => Rating(u, i, ua.getOrElse(globalAverageRating)) }

    return predictions

  }

  /**
    * Computes the predictions on the test set using Item Average.
    *
    * @param train the RDD containing the training Ratings
    * @param test the RDD containing the test (user, item) pairs
    * 
    * @return predictions
    */
  def predictionByItem(train : RDD[Rating], test : RDD[(Int, Int)]) : RDD[Rating] = {
    // Fetch average ratings
    val averageRatingByItem = train.toItemPair.averageByKey

    // Calculate global average rating
    val globalAverageRating = train.averageRating

    val predictions = test
      .map { case (u, i) => (i, u) } // Switch order of PairRDD
      .leftOuterJoin(averageRatingByItem)
      .map { case (i, (u, ia)) => Rating(u, i, ia.getOrElse(globalAverageRating)) }

    return predictions

  }
  
  val perUserMae = maeByPredictor(train, test, predictionByUser)
  val perItemMae = maeByPredictor(train, test, predictionByItem)

  /**
    * Computes x scaled by the user average rating.
    *
    * @param x 
    * @param userAvg
    * 
    * @return x scaled by userAvg
    */
  def scale(x : Double, userAvg : Double) : Double = {
    x match {
      case _ if x > userAvg => (5.0 - userAvg)
      case _ if x < userAvg => (userAvg - 1.0)
      case userAvg => 1.0
    }
  }
  
  /**
    * Compute rating prediction using the baseline method.
    *
    * @param train RDD
    * @param test RDD
    * 
    * @return RDD[Rating] with the predicted rating for each (user, item) pair
    */
  def baselinePrediction(train : RDD[Rating], test : RDD[(Int, Int)]) : RDD[Rating] = {

    // Calculate global average rating
    val globalAvg = train.averageRating

    val userAverageRating = train.toUserPair.averageByKey
    
    val normalizedDeviations = train
      .map(r => (r.user, (r.item, r.rating)))
      .join(userAverageRating)
      .map { case (u, ((i, r), ua)) => Rating(u, i, 1.0 * (r - ua) / scale(r, ua).toDouble) }    
    
    val itemGlobalAverageDeviation = normalizedDeviations.toItemPair.averageByKey

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
            Rating(u, i, (ua + ia * scale((ua + ia), ua)))
        }
      }

    // Verify that all predictions are in the range [1.0, 5.0]
    // assert(predictions.filter(p => (p.rating < 1.0) || (p.rating > 5.0)).count == 0,
    //        "Some predictions are out of bounds")

    return predictions

  } 

  /**
    * Compute the Mean Average Error for the baseline method
    *
    * @param train RDD
    * @param test RDD
    * @param predictor function that uses train and test to predict ratings
    * 
    * @return the MAE using the selected predictor
    */
  def maeByPredictor(
    train : RDD[Rating],
    test : RDD[Rating],
    predictor : (RDD[Rating], RDD[(Int, Int)]) => RDD[Rating]
  ) : Double = {

    val predictionErrors = predictor(train, test.map(r => (r.user, r.item)))
      .map(r => ((r.user, r.item), r.rating))
      .join(test.map(p => ((p.user, p.item), p.rating)))
      .map { case ((u, i), (r, p)) => scala.math.abs(p - r) }

    // Verify that predictions and test RDDs are the same size
    // assert(predictionErrors.count() == test.count(),
    //        "RDD sizes do not match when computing baseline MAE.")
    
    return predictionErrors.mean

  }

  val bonusMae = maeByPredictor(train, test, Recommender.bonusPrediction)

  println(s"MAE using the popularity penalization function from Q4.1.2: $bonusMae")

  val baselineMae = maeByPredictor(train, test, baselinePrediction)

  // Q3.1.5
  val globalTime    = (0 to 10).map(_ => FuncTimer.time({
    train.averageRating
  })).toVector

  val userTime      = (0 to 10).map(_ => FuncTimer.time(
    predictionByUser(train, test.toUserItemPair).collect()
  )).toVector

  val itemTime      = (0 to 10).map(_ => FuncTimer.time(
    predictionByItem(train, test.toUserItemPair).collect()
  )).toVector

  val baselineTime  = (0 to 10).map(_ => FuncTimer.time(
    baselinePrediction(train, test.toUserItemPair).collect()
  )).toVector

  // **************

  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach {
      f => try {
        f.write(content)
      } finally { f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
            "Q3.1.4" -> Map(
              "MaeGlobalMethod" -> globalMae, // Datatype of answer: Double
              "MaePerUserMethod" -> perUserMae, // Datatype of answer: Double
              "MaePerItemMethod" -> perItemMae, // Datatype of answer: Double
              "MaeBaselineMethod" -> baselineMae // Datatype of answer: Double
            ),

            "Q3.1.5" -> Map(
              "DurationInMicrosecForGlobalMethod" -> Map(
                "min" -> globalTime.min,  // Datatype of answer: Double
                "max" -> globalTime.max,  // Datatype of answer: Double
                "average" -> globalTime.mean, // Datatype of answer: Double
                "stddev" -> globalTime.stdev // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerUserMethod" -> Map(
                "min" -> userTime.min,  // Datatype of answer: Double
                "max" -> userTime.max,  // Datatype of answer: Double
                "average" -> userTime.mean, // Datatype of answer: Double
                "stddev" -> userTime.stdev // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerItemMethod" -> Map(
                "min" -> itemTime.min,  // Datatype of answer: Double
                "max" -> itemTime.max,  // Datatype of answer: Double
                "average" -> itemTime.mean, // Datatype of answer: Double
                "stddev" -> itemTime.stdev // Datatype of answer: Double
              ),
              "DurationInMicrosecForBaselineMethod" -> Map(
                "min" -> baselineTime.min,  // Datatype of answer: Double
                "max" -> baselineTime.max,  // Datatype of answer: Double
                "average" -> baselineTime.mean, // Datatype of answer: Double
                "stddev" -> baselineTime.stdev // Datatype of answer: Double
              ),
              "RatioBetweenBaselineMethodAndGlobalMethod" -> baselineTime.mean / globalTime.mean // Datatype of answer: Double
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
