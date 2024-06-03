package org.apache.paimon.spark


import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD

class AskwangScalaITCase extends PaimonSparkTestBase {

  test("spark partitionBy function") {

    val sc = spark.sparkContext
    val rdd = sc.makeRDD(Seq((1, "A"), (2, "B"), (3, "C"), (4, "D")), 2)

    println("------init data-----")
    getMapPartitionsResult(rdd).foreach(println)

    println("-----after partitionBy-----")
    // akwang-todo: 使用 CustomPartitioner 报错 Task not serializable
    val partitionedRdd = rdd.partitionBy(new HashPartitioner(4))
    getMapPartitionsResult(partitionedRdd).foreach(println)
  }

  class CustomPartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = key.asInstanceOf[Int] % partitions
  }

  // 读取rdd每个partition数据
  def getMapPartitionsResult(rdd : RDD[(Int, String)]): Array[(String, List[(Int, String)])] = {
    val part_map = scala.collection.mutable.Map[String, List[(Int, String)]]()
    rdd.mapPartitionsWithIndex {
      (partIdx, iter) => {
        while (iter.hasNext) {
          val part_name = "part_" + partIdx
          val elem = iter.next()
          if (part_map.contains(part_name)) {
            var elems = part_map(part_name)
            elems ::= elem
            part_map(part_name) = elems
          } else {
            part_map(part_name) = List[(Int, String)](elem)
          }
        }
        part_map.iterator
      }
    }.collect()
  }
}
