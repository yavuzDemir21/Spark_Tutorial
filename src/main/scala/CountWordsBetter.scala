


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object CountWordsBetter {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "CountWordsBetter")

    val input = sc.textFile("book.txt")

    val words = input.flatMap(x => x.split("\\W+"))

    val lowerCaseWords = words.map(x => x.toLowerCase())

    val wordCounts = lowerCaseWords.map(x => (x, 1)).reduceByKey((x, y) => x + y)

    val wordCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey().collect()


   // wordCountsSorted.foreach(println)


    for(result <- wordCountsSorted) {
      val count = result._1
      val word = result._2

      println(s"$word : $count")
    }


  }

}
