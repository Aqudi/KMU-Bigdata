package bigdata

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object Task3 {

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val input2 = args(1)
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


    println(s"========== INFO ==========")
    println(s"Input: $input")
    println(s"Input2: $input2")
    println(s"Output: $output\n")

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

    /* Step 1 : Edge 불러와서 Wedge 만들기 */
    println("Step 1: Start")

    // 처음 Edge 를 불러왔을 때는 숫자 그대로의
    // 순서로 정렬을 해준다.
    val edges = sc.textFile(input)
      .map(_.split("\t"))
      .map(x => (x(0).toInt, x(1).toInt))
      .repartition(120)
    val degress = sc.textFile(input2)
      .map(_.split("\t"))
      .map(x => (x(0).toInt, x(1).toInt))
      .repartition(120)

    val edgeWithDegree = degress
      .join(edges)
      .map { case (u, (ud, v)) => (v, (u, ud)) }
      .join(degress, 120)
      .map { case (v, ((u, ud), vd)) => ((u, ud), (v, vd)) }
      .map { case (u, v) =>
        if (u._2 < v._2 || (u._2 == v._2 && u._1 < v._1)) (u, v)
        else (v, u)
      }

    // 하둡에서 처럼 방출된 Edge 들을 Key 노드를 기준으로 묶어준 준뒤
    // Key 노드 중심으로 모인 노드들 중 중복되지 않게 2개를 뽑아 Wedge 로 만들어준다.
    val wedges_save = edgeWithDegree
      .groupByKey()
      .flatMap {
        case (u, vs) =>
          val values = vs.toList
          for {
            i <- values.indices
            j <- i + 1 until values.size
          } yield {
            // u < v 는 이미 edgeWithDegree 를 만들 때 처리했으므로
            // 마지막 Triangle 을 세기 위해 edges(Task1에서 이미 정렬되있는 상태)
            // 와 똑같이 숫자 그대로 순서로 정렬해준다.
            val vw = (values(i)._1, values(j)._1)
            if (vw._1 < vw._2) (vw, u._1)
            else (vw.swap, u._1)
          }
      }
      .groupByKey()
    wedges_save
      .map { case ((u, v), ws) => u + "\t" + v + "\t" + ws.mkString("\t") }
      .saveAsTextFile(temp)
    println("Step 1: End\n")
    println(s"wedges: ${wedges_save.count()}")

    /* Step 2 : Wedge 와 Edge 를 모두 Input 으로 받아 삼각형 찾기 */
    println("Step 2: Start")
    val wedges = sc.textFile(temp)
      .map(_.split("\t"))
      .map { x => ((x(0).toInt, x(1).toInt), x.slice(2, x.length).toSeq) }

    // Wedge 를 기준으로 노드들을 모아  Wedge 를 닫아 삼각형을 만들 수 있는지 확인한다.
    val triangles = edges.map { case (u, v) => ((u, v), Seq(-1)) }
      .join(wedges, 120)
      .flatMapValues(x => x._2)
    // triangles.foreach(println)
    println("Step 2: End\n")

    println("Step 3: Start")
    val counted = triangles
      .flatMap(x => Seq(x._1._1, x._1._2, x._2))
      .countByValue()
    val counted_save = counted.map(_.productIterator.mkString("\t")).toSeq.sortBy(x => x.split("\t")(0).toInt)
    sc.parallelize(counted_save).saveAsTextFile(output)
    println("삼각형: " + triangles.count() + "개")
    println("Step 3: End\n")
  }

}
