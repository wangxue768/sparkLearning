package spark.core.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark02_WordCount {

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

    //根据元组的第一个值也就是word进行分组。 (Hello,CompactBuffer((Hello,1), (Hello,1), (Hello,1), (Hello,1)))
    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
      t => t._1  // t._1 表示元组的第一个值,
    )

    // 4.对分组后的数据进行转换
    //(hello, 3) (scala, 1)

    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        val wordCount: (String, Int) = list.reduce(  //reduce默认使用reduceLeft表示从集合头部开始操作
          (t1, t2) =>
            (t1._1, t1._2 + t2._2)
        )
        wordCount //(hello, 4)

      }
    }

    // 5.将采集结果打印到控制台

    val array: Array[(String, Int)] = wordToCount.collect() //采集

    array.foreach(println)


    // TODO 3. 关闭连接
    sc.stop()




  }

}
