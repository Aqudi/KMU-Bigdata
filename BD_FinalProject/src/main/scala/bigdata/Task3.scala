package bigdata

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object Task3 {

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val degrees = args(1)
    val output = args(2)

    //  Spark settings
    val conf = new SparkConf().setAppName("Final project app").setMaster("local")
    val sc = new SparkContext(conf)
//    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    /* Degree 정보를 사용해서 정렬할 수 있도록 세팅 */
    //    val degree_db = sc.textFile(degrees + "/*")
    //      .map(_.split("\t"))
    //      .map(a => (a(0), a(1)))
    //      .toDF("vertex", "degree")
    //      .cache()

    println(s"\nInput: $input")
    println(s"Input2: $degrees")
    println(s"Output: $output\n")


    /* Step 1 : Edge 불러와서 Wedge 찾기 */
    println("Step 1: Start")
    val inputRDD = sc.textFile(input + "/*")
    val edges = inputRDD
      .map {
        line =>
          // Edge 를 불러와 Split 한 후 edge 를 만들어 return 해준다.
          val edge = line.split("\t")
          val u = edge(0).toInt
          val v = edge(1).toInt
          (u, v)
      }

    // 하둡에서 처럼 방출된 Edge 들을 Key vertex 기준으로 묶은 뒤
    val wedges = edges
      .groupByKey()
      .map {
        edge =>
          val u = edge._1.toInt
          val values = edge._2.map(_.toInt).toList
          val result = for {
            i <- values.indices
            j <- i + 1 until values.size
          } yield {
            ((values(i), values(j)), u)
          }
          result
      }
      .filter(_.nonEmpty)
      .flatMap(v => v)
      .groupByKey()
    println("Step 1: End\n")

    /* Step 2 : Wedge 와 Edge 를 모두 Input 으로 받아 삼각형 찾기 */
    println("Step 2: Start")
    val f_edges = edges.map(x => (x, -1)).groupByKey()
    val f_input = wedges.union(f_edges)
    //        println("wedgs", wedges.foreach(println))
    //        println("edges", edges.foreach(println))

    val triangles = f_input
      .groupByKey()
      .flatMap {
        tri =>
          val centers = tri._2.flatten
          var result = IndexedSeq(Seq(-1, -1, -1))
          if (centers.exists(x => x == -1)) {
            val c_vs = centers.filter(x => x != -1).toList
            val joined = for {
              i <- c_vs.indices
            } yield IndexedSeq(tri._1._1, tri._1._2, c_vs(i))
            result = joined
          }
          result.toList
      }
      .filter(x => x.head != -1)
    println("Step 2: End\n")

    println("Step 3: Start\n")
    // 파일로 저장
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outputPath = new Path(output)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }

    val counted = triangles
      .flatMap(x => x)
      .sortBy(x=>x)
      .countByValue()
    val counted_save = counted.map(_.productIterator.mkString("\t")).toSeq.sortBy(x=>x.split("\t")(0).toInt)
    sc.parallelize(counted_save).coalesce(1).saveAsTextFile(output)

    println("Step 3: End\n")


     // 총 삼각형 개수 출력
      println("삼각형: " + counted.values.sum / 3 + "개")
    sc.stop()
  }
}
