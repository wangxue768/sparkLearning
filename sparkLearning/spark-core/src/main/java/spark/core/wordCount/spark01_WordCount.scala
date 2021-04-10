package spark.core.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark01_WordCount {

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
    // flatMap通常用于切词
    val words: RDD[String] = lines.flatMap(_.split(" ")) //扁平映射，行字符串变成单词，以空格分割 _表示对于每一个

    // 3.将数据根据单词进行分组，进行统计
    //(hello, hello, hello) (scala)(spark)
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    // 4.对分组后的数据进行转换
    //(hello, 3) (scala, 1)

    val wordToCount = wordGroup.map { //对结构转变一般要用map
      case (word, list) => {
        (word, list.size)
      }
    }

    // 5.将采集结果打印到控制台

    val array: Array[(String, Int)] = wordToCount.collect() //采集

    array.foreach(println)


    // TODO 3. 关闭连接
    sc.stop()



  }

}
