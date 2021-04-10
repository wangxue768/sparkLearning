package spark.core.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark03_WordCount {

  def main(args: Array[String]): Unit = {

    // TODO 1. 建立和spark的连接

    // spark的基础配置对象
    val sparkConf = new SparkConf()
      .setMaster("local")  //spark框架的运行环境
      .setAppName("WordCount") //spark的程序的名称

    val sc = new SparkContext(sparkConf)

    // TODO 2. 执行业务操作
    // 1 读取文件，获取每行数据
    // hello word
    val lines: RDD[String] = sc.textFile("data/*")

    // 2.将每行数据进行拆分，形成分词效果
    // 扁平化： 将整体拆成个体的操作
    val words: RDD[String] = lines.flatMap(_.split(" ")) //扁平映射，行字符串变成单词，以空格分割 _表示对于每一个

    val wordToOne: RDD[(String, Int)] = words.map {
      word => (word, 1) //将每一个word转换成元组的形式，(word, 1)表示每个单词出现了一次
    }

    /**
     * Spark框架提供了更多的功能，可以将分组和聚合使用一个方法
     */
    //reduceByKey：相同的key可以对value进行reduce聚合
    val wordCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _) //变量只使用了一次可以用下划线代替，至简原则

    val array: Array[(String, Int)] = wordCount.collect()

    array.foreach(println)
    // TODO 3. 关闭连接
    sc.stop()




  }

}
