package bigdata

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object Task4 {

  def main(args: Array[String]): Unit = {
    val task2 = args(0)
    val task3 = args(1)
    val output = args(2)
    val temp = output + ".tmp"

    var master = "yarn"
    if (args.length >= 4) {
      if (args(3).equals("local")) {
        master = "local";
      }
    }

    //  Spark settings
    val conf = new SparkConf()
      .setAppName("Final project app")
      .setMaster(master)
    val sc = new SparkContext(conf)

    // 만약 이전 결과가 남아있다면 제거
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outputPath = new Path(output)
    val tempPath = new Path(temp)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }
    if (fs.exists(tempPath)) {
      fs.delete(tempPath, true)
    }

    val v_t = sc.textFile(task3)
      .map(_.split("\t"))
      .map(x => (x(0).toInt, x(1).toDouble))
      .repartition(120)
    val v_d = sc.textFile(task2)
      .map(_.split("\t"))
      .map(x => (x(0).toInt, x(1).toDouble))
      .repartition(120)

    // 군집계수 = 삼각형수 * 2 / 이웃한노드수 * (이웃한노드수-1)
    val cc = v_t.join(v_d)
      .map{ case (v, (t, d)) => (v, (t.toDouble * 2) / (d.toDouble * (d.toDouble - 1)))}
      .sortByKey()
      .map{ case (v, cc) => s"$v\t$cc"}
      .coalesce(1)
      .saveAsTextFile(output)
  }
}
