import org.apache.spark.{SparkConf, SparkContext}


object Task3 {
  def main(args: Array[String]): Unit = {
    val logFile = "README.md"

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")

    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile, 2).cache()

    val lineCount = logData.count()

    println(lineCount)

    sc.stop()
  }
}
