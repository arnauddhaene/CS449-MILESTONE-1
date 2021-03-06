package predict

import stats.Analyzer
import stats.Rating

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

case class BaselineRating(user : Int, item : Int, rating: Double, userAvg: Double, itemAvg: Double)

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
  //  MY CODE HERE
  // **************

  // Q3.1.4
  
  val globalAverageRating = train.map(rating => rating.rating).mean
  val globalMae = test
    .map(r => scala.math.abs(r.rating - globalAverageRating))
    .reduce(_ + _) / test.count.toDouble
  
  /**
    * Computes the MAE (Mean Average Error) using the average prediction method.
    *
    * @param train the RDD containing the training samples.
    * @param test the RDD containing the testing samples.
    * @param onKey the key to average on: one of {"user","item"}.
    * 
    * @return the MAE of the predictions on the test.
    */
    def maeByAveragePer(train : RDD[Rating], test : RDD[Rating], onKey : String) : Double = {

    // Fetch average ratings
    val averageRating = Analyzer.averageRatingPer(train, onKey)

    // Calculate global average rating
    val globalAverageRating = train.map(r => r.rating).mean

    val mae = test
      .map(r => (if(onKey == "item") r.item else r.user, r.rating))
      .leftOuterJoin(averageRating)
      .map { case (k, (v, w)) => scala.math.abs(v - w.getOrElse(globalAverageRating)) }

    // Verify that all test ratings were compared to our predictions before averaging
    assert(mae.count() == test.count(), s"RDD sizes do not match when computing per $onKey MAE.")

    return mae.reduce(_ + _) / mae.count.toDouble

  }
  
  print("perUserMae \t")
  val perUserMae = spark.time(maeByAveragePer(train, test, "user"))

  print("perItemMae \t")
  val perItemMae = spark.time(maeByAveragePer(train, test, "item"))

  def scale(x : Double, userAvg : Double) : Double = {
    x match {
      case a if x > userAvg => 5 - userAvg
      case a if x < userAvg => userAvg - 1
      case userAvg => 1
    }
  }
  
  def maeByBaseline(train : RDD[Rating], test : RDD[Rating]) : Double = {
    val userAverageRating = Analyzer.averageRatingPer(train, "user")
    
    val normalizedDeviations = train
      .map(r => (r.user, r))
      .join(userAverageRating)
      .map { case (k, (v, w)) => Rating(k, v.item, (v.rating - w) / scale(v.rating, w))}
    
    val itemGlobalAverageDeviation = Analyzer.averageRatingPer(normalizedDeviations, "item")
    
    val baselineErrors = test
      .map(r => (r.user, BaselineRating(r.user, r.item, r.rating, -999.0, -999.0)))
      .leftOuterJoin(userAverageRating)
      .map { case (u, (r, uAvg)) => (r.item, BaselineRating(u, r.item, r.rating, uAvg.getOrElse(globalAverageRating), -999.0))}
      .leftOuterJoin(itemGlobalAverageDeviation)
      .map { case (i, (b, iAvg)) => BaselineRating(b.user, i, b.rating, b.userAvg, iAvg.getOrElse(globalAverageRating))}
      .map(b => scala.math.abs(b.rating - (b.userAvg + b.itemAvg * scale((b.userAvg + b.itemAvg), b.userAvg))))
    
    assert(baselineErrors.count() == test.count(), s"RDD sizes do not match when computing baseline MAE.")
    
    return baselineErrors.reduce(_ + _) / baselineErrors.count.toDouble
  }

  print("baselineMethod \t")
  val baselineMae = spark.time(maeByBaseline(train, test))

  // Q3.1.5
  def mean(underlying : Vector[Double]) : Double = {
    return underlying.reduce(_ + _) / underlying.size.toDouble
  }
  
  def stdDev(underlying : Vector[Double]) : Double = {
      return scala.math.sqrt(underlying
        .map(_ - mean(underlying)).map(t => t * t)
        .reduce(_ + _) / underlying.size.toDouble)
  }

  def calculateGlobalTime() : Double = {
    val start = System.nanoTime()
    val globalAverageRating = train.map(rating => rating.rating).mean
    val globalMae = test
      .map(r => scala.math.abs(r.rating - globalAverageRating))
      .reduce(_ + _) / test.count.toDouble
    return (System.nanoTime() - start) / 1e3d
  }

  def calculateTimePer(onKey : String) : Double = {
    val start = System.nanoTime()
    maeByAveragePer(train, test, onKey)
    return (System.nanoTime() - start) / 1e3d
  }

  def calculateTimeBaseline() : Double = {
    val start = System.nanoTime()
    maeByBaseline(train, test)
    return (System.nanoTime() - start) / 1e3d
  }

  val globalTime    = Range(0, 10, 1).map(_ => calculateGlobalTime()).toVector
  val userTime      = Range(0, 10, 1).map(_ => calculateTimePer("user")).toVector
  val itemTime      = Range(0, 10, 1).map(_ => calculateTimePer("item")).toVector
  val baselineTime  = Range(0, 10, 1).map(_ => calculateTimeBaseline()).toVector

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
                "average" -> mean(globalTime), // Datatype of answer: Double
                "stddev" -> stdDev(globalTime) // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerUserMethod" -> Map(
                "min" -> userTime.min,  // Datatype of answer: Double
                "max" -> userTime.max,  // Datatype of answer: Double
                "average" -> mean(userTime), // Datatype of answer: Double
                "stddev" -> stdDev(userTime) // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerItemMethod" -> Map(
                "min" -> itemTime.min,  // Datatype of answer: Double
                "max" -> itemTime.max,  // Datatype of answer: Double
                "average" -> mean(itemTime), // Datatype of answer: Double
                "stddev" -> stdDev(itemTime) // Datatype of answer: Double
              ),
              "DurationInMicrosecForBaselineMethod" -> Map(
                "min" -> baselineTime.min,  // Datatype of answer: Double
                "max" -> baselineTime.max,  // Datatype of answer: Double
                "average" -> mean(baselineTime), // Datatype of answer: Double
                "stddev" -> stdDev(baselineTime) // Datatype of answer: Double
              ),
              "RatioBetweenBaselineMethodAndGlobalMethod" -> mean(baselineTime) / mean(globalTime) // Datatype of answer: Double
            ),
         )
        json = Serialization.writePretty(answers)
      }

      // println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
