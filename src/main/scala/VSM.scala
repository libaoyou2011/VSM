// 丁嘉瑞
// version：1.0
// 功能基本完成，没有进行流水并行设计

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
		val doc = sc.textFile("/Users/EdwardDing/Documents/SCALA/VSM/doc_single").flatMap(line => line.split(" "))
		val sumD = doc.map(number => number.toDouble * number.toDouble).reduce(_ + _)
		val distD = Math.sqrt(sumD)

		// 处理query的距离
		// 仅需处理一次
		val query = sc.textFile("/Users/EdwardDing/Documents/SCALA/VSM/query").flatMap(line => line.split(" "))
		val sumQ = query.map(number => number.toDouble * number.toDouble).reduce(_ + _)
		val distQ = Math.sqrt(sumQ)

		// 计算分母
		val denom = distQ * distD

		if (doc.count != query.count) {
			println("### ERROR: Part of data is missing.")
		}
		else 
		{
			// 计算VSM算法中的分子部分
			val tempZip = query.zip(doc)
			val numer = tempZip.map(pair => pair._1.toDouble * pair._2.toDouble).reduce(_ + _)

			// 得到相似度结果
			val sim = numer / denom
			println("### OUTPUT: The similarity is: " + sim)
		}
	}

	def mapFun(line: String) = {
		
	}
}
