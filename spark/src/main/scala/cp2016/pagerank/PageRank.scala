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
      var links = linkPattern.findAllIn(text)
                             .toList
                             .map { link => link.substring(2, link.length() - 2).split(linkSplitPattern) }
                             .filter { arr => arr.size > 0 }
                             .map { arr => (title, arr(0)) }
                             
      links.union(List((title, "")))
    }
    
    val n = adjMatrix.map(_._1).distinct().map(_ => 1).sum()
    val numDocs = ctx.broadcast(n) 
    
    adjMatrix = adjMatrix.map(x => (x._2, x._1))
                         .leftOuterJoin(adjMatrix, ctx.defaultParallelism * 12)
                         .filter(x => x._1 == "" || !x._2._2.isEmpty)
                         .filter(x => !x._2._2.exists(e => e == ""))
                         .map(x => (x._2._1, x._1))
                      
    var tmpAdjMat = adjMatrix.map(tup => (tup._1, List(tup._2)))
                             .reduceByKey(_ ++ _, ctx.defaultParallelism * 12)
    
    var adjMat = tmpAdjMat.map(tup => (tup._1, (1.0 / n, tup._2)))
    var diff = 0.0
    var iter = 0
    do {
      var sinkNodeRankSum = adjMat.filter(tup => tup._2._2.size == 1)
                                .map(tup => tup._2._1)
                                .sum
      sinkNodeRankSum = sinkNodeRankSum / numDocs.value * 0.85
    
      val teleport = 0.15 * (1.0 / numDocs.value);
      
      var matz = adjMat.flatMap { tup =>
        val neighbors = tup._2._2
        val pr = tup._2._1
        neighbors.map { n =>
          if (n.size == 0) {
            (tup._1, (0.0, neighbors))
          } else {
            (n, (pr / (neighbors.size - 1) * 0.85, List()))
          }
        }
      }.reduceByKey((a, b) => ((a._1 + b._1), a._2 ++ b._2), adjMat.getNumPartitions)
       .map(tup => (tup._1, (tup._2._1 + sinkNodeRankSum + teleport, tup._2._2)))
      
      diff = matz.union(adjMat)
                 .reduceByKey((a, b) =>(math.abs(a._1 - b._1), List()), adjMat.getNumPartitions)
                 .map(tup => tup._2._1).sum()
      
      adjMat = matz
    } while(diff >= 0.001)
    
    adjMat.sortBy(tup => (-tup._2._1, tup._1), true, ctx.defaultParallelism * 12)
          .map(tup => tup._1 + "\t" + tup._2._1.toString())
          .saveAsTextFile(outputDir)
          
    try { ctx.stop } catch { case _ : Throwable => {} }
  }
}
