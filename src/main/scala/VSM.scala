// 丁嘉瑞
// version：1.1

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source

object VSM {
def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("VSM")
		val sc = new SparkContext(conf)

		// 预处理doc，获取doc向量的距离
		// 这一部分应当另写程序处理，并将doc库的距离统一放到一个文件中
		val doc = sc.textFile("/Users/EdwardDing/VSM/doc").map(line => line.split(" ")).collect
		val sumD = 
		for (line <- doc)
			yield line.map(s => s.toDouble * s.toDouble).reduce(_ + _)
		val distD = sumD.map(x => Math.sqrt(x))

		// 处理query的距离
		// 仅需处理一次
		val query = sc.textFile("/Users/EdwardDing/VSM/query").flatMap(line => line.split(" ")).collect
		val sumQ = query.map(s => s.toDouble * s.toDouble).reduce(_ + _)
		val distQ = Math.sqrt(sumQ)

		// 计算分母
		val denom = distD.map(x => x * distQ)

		// 计算分子
		val numer = 
		for (line <- doc)
			yield line.zip(query).map(pair => pair._1.toDouble * pair._2.toDouble).reduce(_ + _)

		// 计算相似度
		val sim = numer.zip(denom).map(pair => pair._1 / pair._2)
		sim.foreach(x => println(f"### Output: Similarity = $x%.3f"))
	}
}
