import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("u.data")

/*
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.toString().split("\t")(2))

    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
    sortedResults.foreach(println)

    val users = lines.map(x => x.toString().split("\t")(0)).map(x => x.toInt)

    val userCount = users.distinct().count()

    println(userCount)

    val userRatingCount = users.countByValue()

    val sortedUserRatingCount = userRatingCount.toSeq.sortBy(_._2).reverse

    println(sortedUserRatingCount.head)
*/


    val lines_separated = lines.map(x => x.toString().split("\t"))

    val movie_rating = lines_separated.map(x => (x(1).toInt, x(2).toInt))

   // movie_rating.foreach(println)

    val grouped_movie_rating = movie_rating.groupByKey()

    //grouped_movie_rating.foreach(println)

    val movie_meanrating = grouped_movie_rating.map(x => (x._1, mean(x._2.toList)))

    val ordered_meanrating = movie_meanrating.toLocalIterator.toSeq.sortBy(_._1)


    ordered_meanrating.foreach(println)


  }

  def mean(list: List[Int]): Double = {

    1.0*list.reduce((x, y) => x+y)/list.length
  }

}