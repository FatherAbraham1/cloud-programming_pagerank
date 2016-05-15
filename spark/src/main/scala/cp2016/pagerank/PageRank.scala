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
    var adjMatrix = pages.flatMap { line =>
      val xml = XML.loadString(line)
      val title = (xml \\ "title").text.capitalize
      val text = (xml \\ "text").text
      var links = linkPattern.findAllIn(text).toList.map { link =>
        (title, link.substring(2, link.length() - 2).split(linkSplitPattern)(0).capitalize)
      }
      links.union(List((title, "")))
    }
    
    val keySet = adjMatrix.map(_._1).distinct()
    val keys = ctx.broadcast(keySet)
    
    val invalidLinks = adjMatrix.fullOuterJoin(adjMatrix)
                                .filter(x => x._2._2.isEmpty)
                                .map(x => x._2._1.get).distinct().collect().toSet
    val bye = ctx.broadcast(invalidLinks)
    adjMatrix = adjMatrix.filter(x => !bye.value.contains(x._2))
    
    adjMatrix.map(x => x._1).distinct().saveAsTextFile(outputDir)

    ctx.stop
  }
}
