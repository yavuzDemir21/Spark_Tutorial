import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level



object main extends App{


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka"). setLevel(Level.OFF)

  val spark = SparkSession.builder().appName("alper").master("local[*]").getOrCreate()

  import spark.implicits._

  val someDF = Seq(
    (8, "bat"),
    (64, "mouse"),
    (-27, "horse")
  ).toDF("number", "word")

  someDF.show()



}
