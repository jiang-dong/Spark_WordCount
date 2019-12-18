import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class WordCounts {

  // 1. 创建 Spark Context
  val conf = new SparkConf().setMaster("local[4]").setAppName("wordcount")
  val sc: SparkContext = new SparkContext(conf)


  def main(args: Array[String]): Unit = {

    // 2. 读取文件并计算词频
    val source: RDD[String] = sc.textFile("dataset/wordcount.txt", 2)

   // val source: RDD[String] = sc.textFile("hdfs://hadoop01:8020/export/data/wordcount.txt", 2)
    val words: RDD[String] = source.flatMap { line => line.split(" ") }
    val wordsTuple: RDD[(String, Int)] = words.map { word => (word, 1) }
    val wordsCount: RDD[(String, Int)] = wordsTuple.reduceByKey { (x, y) => x + y }

    // 3. 查看执行结果
    //println(wordsCount.collect)
    wordsCount.collect.foreach(println(_))
  }


  /**
    * 本地集合创建rdd,分区数默认local[N]的N
    */
  @Test
  def createRddFromLocal(): Unit ={
    val data = Seq[Int](1,2,3,4,5)
    val rdd1=sc.parallelize(data,2)

    //查看rdd的分区数
    println(rdd1.partitions.length)
  }


  /**
    * 读取外部文件创建RDD
    */
  @Test
  def createRddFromFile(): Unit ={
    val rdd1 = sc.textFile("dataset/wordcount.txt",6)

    println(rdd1.partitions.length)
  }


  def createRddFromRdd(): Unit ={
    val rdd1 = sc.textFile("dataset/wordcount.txt",6)

    println("rdd1的分区数="+rdd1.partitions.length)

    val rdd2 = rdd1.flatMap(_.split(" "))

    println("rdd2的分区数="+rdd2.partitions.length)

  }

  @Test
  def reduceByKey(): Unit ={
    val data = Seq[(String,Int)](("hadoop",10),("spark",5),("spark",3),("hadoop",20),("hadoop",30),("flume",3))
    val rdd1 = sc.parallelize(data,1)

    rdd1.reduceByKey((agg,curr)=>{
      println(agg,curr)
      agg+curr
    }).collect()

  }


}

