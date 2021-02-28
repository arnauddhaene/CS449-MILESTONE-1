package predict

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

case class Rating(user: Int, item: Int, rating: Double)

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
  
  val globalAverageRating = train.map(rating => rating.rating).mean
  val globalMae = test.map(r => scala.math.abs(r.rating - globalAverageRating))
    .reduce(_ + _) / test.count.toDouble

  /**
    * Computes the average rating per item or user.
    *
    * @param data the RDD containing Ratings.
    * @param onKey the key to average on: one of {"user","item"}.
    * 
    * @return a RDD of Tuple2[Int, Double] with the ID of {"user","item"} and the rating.
    */
  def averageRatingPer(data : RDD[Rating], onKey : String) : RDD[Tuple2[Int, Double]] = {
    return data.map(r => (if(onKey == "item") r.item else r.user, r.rating))
      .mapValues(r => (r, 1))
      .reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR) }
      .mapValues { case (sum, count) => 1.0 * sum / count }
  }
  
  /**
    * Computes the MAE (Mean Average Error) using the average prediction method.
    *
    * @param train the RDD containing the training samples.
    * @param test the RDD containing the testing samples.
    * @param onKey the key to average on: one of {"user","item"}.
    * 
    * @return the MAE of the predictions on the test.
    */
  def MaeByAveragePer(train : RDD[Rating], test : RDD[Rating], onKey : String) : Double = {

    // Fetch average ratings
    val averageRating = averageRatingPer(train, onKey)

    // Calculate global average rating
    val globalAverageRating = train.map(r => r.rating).mean

    // Find items or users in test missing in train and give them global average rating
    val testWithMissing = test.map(r => if (onKey == "item") r.item else r.user)
      .subtract(averageRating.map(r => r._1))
      .map(r => (r, globalAverageRating))
      .union(averageRating)

    val mae = test.map(r => (if(onKey == "item") r.item else r.user, r.rating))
      .join(testWithMissing)
      .map { case (k, (v, w)) => scala.math.abs(v - w) }

    assert(mae.count() == test.count(), s"RDD sizes do not match when computing per $onKey MAE.")

    return mae.reduce(_ + _) / mae.count.toDouble

  }
  
  val perUserMae = MaeByAveragePer(train, test, "user")
  val perItemMae = MaeByAveragePer(train, test, "item")

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
              "MaeBaselineMethod" -> 0.0 // Datatype of answer: Double
            ),

            "Q3.1.5" -> Map(
              "DurationInMicrosecForGlobalMethod" -> Map(
                "min" -> 0.0,  // Datatype of answer: Double
                "max" -> 0.0,  // Datatype of answer: Double
                "average" -> 0.0 // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerUserMethod" -> Map(
                "min" -> 0.0,  // Datatype of answer: Double
                "max" -> 0.0,  // Datatype of answer: Double
                "average" -> 0.0 // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerItemMethod" -> Map(
                "min" -> 0.0,  // Datatype of answer: Double
                "max" -> 0.0,  // Datatype of answer: Double
                "average" -> 0.0 // Datatype of answer: Double
              ),
              "DurationInMicrosecForBaselineMethod" -> Map(
                "min" -> 0.0,  // Datatype of answer: Double
                "max" -> 0.0, // Datatype of answer: Double
                "average" -> 0.0 // Datatype of answer: Double
              ),
              "RatioBetweenBaselineMethodAndGlobalMethod" -> 0.0 // Datatype of answer: Double
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
