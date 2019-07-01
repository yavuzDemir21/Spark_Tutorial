
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.Vectors


object LinearRegressionDataFrame {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession
      .builder
      .appName("LinearRegressionDF")
      .master("local[*]")
      .getOrCreate()



    val input = spark.sparkContext.textFile("regression.txt")
    //format expected from mllib
    val data = input.map(_.split(",")).map(x => (x(0).toDouble, Vectors.dense(x(1).toDouble)))

    import spark.implicits._

    val colNames = Seq("label", "features")
    val df = data.toDF(colNames: _*)


    val trainTest = df.randomSplit(Array(0.5, 0.5))

    val trainingDF = trainTest(0)
    val testingDF = trainTest(1)


    val lir = new LinearRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setMaxIter(100)
      .setTol(1E-6)

    // Train the model using our training data
    val model = lir.fit(trainingDF)

    // Now see if we can predict values in our test data.
    // Generate predictions using our linear regression model for all features in our
    // test dataframe:
    val fullPredictions = model.transform(testingDF).cache()

    // This basically adds a "prediction" column to our testDF dataframe.

    // Extract the predictions and the "known" correct labels.
    val predictionAndLabel = fullPredictions.select("prediction", "label").rdd.map(x => (x.getDouble(0), x.getDouble(1)))

    // Print out the predicted and actual values for each point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }

    // Stop the session
    spark.stop()


  }

}
