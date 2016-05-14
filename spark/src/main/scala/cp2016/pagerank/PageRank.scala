package cp2016.pagerank

import scala.xml._
import org.apache.spark._
import org.apache.hadoop.fs._

object PageRank {
  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputDir = args(1)

    val config = new SparkConf().setAppName("PageRank")
    val ctx = new SparkContext(config)

    // clean output directory
    val hadoopConf = ctx.hadoopConfiguration
    var hdfs = FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new Path(outputDir), true)
    } catch {
      case ex : Throwable => {
        println(ex.getMessage)
      }
    }
    
    val pages = ctx.textFile(inputPath, ctx.defaultParallelism)
    
    val linkPattern = """\[\[[^\]]+\]\]""".r
    val linkSplitPattern = "[#|]"
    val adjMatrix = pages.map { line =>  
      val xml = XML.loadString(line)
      val text = (xml \ "text").text
      var links = ctx.parallelize(linkPattern.findAllIn(text).toList, ctx.defaultParallelism)
      links = links.map { link =>
        link.substring(2, link.length() - 2).split(linkSplitPattern)(0)
      }.filter { link => !link.isEmpty() }
      ((xml \ "title").text, links)
    }
    
    adjMatrix.map { a => (a._1, a._2.toString()) }.saveAsTextFile(outputDir)
    
    ctx.stop
  }
}
