

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object CountWords {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "CountWords")

    val input = sc.textFile("/home/yavuz.demir/Downloads/SparkScala/book.txt")

//    val words = input.flatMap(x => x.split(" "))

    val words = input.flatMap(x => x.split("\\W+")).map(x=>x.toLowerCase())

    val wordCounts = words.countByValue()

    wordCounts.foreach(println)


  }


}
