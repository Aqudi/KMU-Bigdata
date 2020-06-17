package bigdata

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object Task3 {

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val input2 = args(1)
    val output = args(2)

    //  Spark settings
    val conf = new SparkConf()
      .setAppName("Final project app")
      .set("spark.executors.cores", "2")
      .set("spark.executor.memory", "3g")
//      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")


    println(s"========== INFO ==========")
    println(s"Input: $input")
    println(s"Input2: $input2")
    println(s"Output: $output\n")

    // 만약 이전 결과가 남아있다면 제거
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outputPath = new Path(output)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }

    /* Step 1 : Edge 불러와서 Wedge 만들기 */
    println("Step 1: Start")

    // 처음 Edge 를 불러왔을 때는 숫자 그대로의
    // 순서로 정렬을 해준다.
    val edges = sc.textFile(input + "/part-r-*")
      .map(_.split("\t"))
      .map(x => (x(0).toInt, x(1).toInt))
    val degress = sc.textFile(input2 + "/part-r-*")
      .map(_.split("\t"))
      .map(x => (x(0).toInt, x(1).toInt))

    val edgeWithDegree = degress
      .join(edges)
      .map { case (u, (ud, v)) => (v, (u, ud)) }
      .join(degress)
      .map { case (v, ((u, ud), vd)) => ((u, ud), (v, vd)) }
      .map { case (u, v) =>
        if (u._2 < v._2) (u, v)
        else (v, u)
      }

    // 하둡에서 처럼 방출된 Edge 들을 Key 노드를 기준으로 묶어준 준뒤
    // Key 노드 중심으로 모인 노드들 중 중복되지 않게 2개를 뽑아 Wedge 로 만들어준다.
    val wedges = edgeWithDegree
      .groupByKey()
      .map {
        case ((u, ud), vs) =>
          val values = vs.toList
          val result = for {
            i <- values.indices
            j <- i + 1 until values.size
          } yield {
            // u < v 는 이미 edgeWithDegree 를 만들 때 처리했으므로
            // 마지막 Triangle 을 세기 위해 edges(Task1에서 이미 정렬되있는 상태) 와 똑같이
            // 숫자 그대로 순서로 정렬해준다.
            val v_info = values(i)
            val w_info = values(j)
            var vw = (v_info._1, w_info._1)
            if (v_info._1 > w_info._1) {
              vw = vw.swap
            }
            (vw, u)
          }
          //          result.foreach(println)
          result
      }
      .flatMap(v => v)
      .groupByKey()
    println(s"wedges: ${wedges.count()}")
    println("Step 1: End\n")

    /* Step 2 : Wedge 와 Edge 를 모두 Input 으로 받아 삼각형 찾기 */
    println("Step 2: Start")
    // Wedge 가 정렬된 상태이므로 Edge 도 정렬을 해줘서
    // 같은 Key 값으로 모일 수 있도록 해준다.
    val triangles = edges
      .map { case (u, v) => ((u, v), Iterable(-1)) }
      .union(wedges)
      .groupByKey()
      .flatMap {
        tri =>
      val centers = tri._2.flatten
      val emit = centers.exists(x => x == -1)
      val c_vs = centers.filter(x => x != -1).toList
      val joined = for {
        i <- c_vs.indices
        if emit
      } yield IndexedSeq(tri._1._1, tri._1._2, c_vs(i))
      joined
  }
  println("Step 2: End\n")

  println("Step 3: Start\n")
    val counted = triangles
      .flatMap(x => x)
      .countByValue()
    val counted_save = counted.map(_.productIterator.mkString("\t")).toSeq.sortBy(x => x.split("\t")(0).toInt)
    sc.parallelize(counted_save).coalesce(1).saveAsTextFile(output)
    //    println("삼각형: " + counted.values.sum / 3 + "개")
    println("삼각형: " + triangles.count() + "개")
    println("Step 3: End\n")

    // 총 삼각형 개수 출력
    // facebook: 1612010개
  }

}
