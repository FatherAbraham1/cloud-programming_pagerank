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
    
    val invalidLinks = adjMatrix.fullOuterJoin(adjMatrix)
                                .filter(x => x._2._2.isEmpty)
                                .map(x => x._2._1.get).distinct().collect().toSet
    val bye = ctx.broadcast(invalidLinks)
    adjMatrix = adjMatrix.filter(x => !bye.value.contains(x._2))
    var tmpAdjMat = adjMatrix.map(tup => (tup._1, List(tup._2)))
                             .reduceByKey(_ ++ _)
    
    var adjMat = tmpAdjMat.map(tup => (tup._1, (1.0 / n, tup._2)))
    var matz = adjMat.cache()
    var diff = 0.0
    var iter = 0
    do {
      
      var sinkNodeRankSum = matz.filter(tup => tup._2._2.size == 1)
                                .map(tup => tup._2._1)
                                .sum
      sinkNodeRankSum = sinkNodeRankSum / numDocs.value * 0.85
      val snkVal = ctx.broadcast(sinkNodeRankSum);
    
      val teleport = ctx.broadcast(0.15 * (1.0 / numDocs.value));
    
      adjMat = matz.flatMap { tup =>
        val neighbors = tup._2._2
        val pr = tup._2._1
        neighbors.map { n =>
          if (n.size == 0) {
            (tup._1, (0.0, neighbors))
          } else {
            (n, (pr / (neighbors.size - 1) * 0.85, List()))
          }
        }
      }.reduceByKey{ (a, b) => 
        ((a._1 + b._1), a._2 ++ b._2)
      }.map(tup => (tup._1, (tup._2._1 + snkVal.value + teleport.value, tup._2._2)))
      
      diff = matz.union(adjMat).reduceByKey { (a, b) =>
        (math.abs(a._1 - b._1), List())
      }.map(tup => tup._2._1).sum()
      
      matz.unpersist(false)
      matz = adjMat.cache()
      ctx.parallelize(List((iter.toString() + " : " + diff.toString())), 1).saveAsTextFile("tmp/iter" + iter.toString())
      iter += 1
    } while(diff >= 0.001)
    
    adjMat.sortBy(tup => (-tup._2._1, tup._1), true, ctx.defaultParallelism * 3)
          .map(tup => tup._1 + "\t" + tup._2._1.toString())
          .saveAsTextFile(outputDir)
          
    try { ctx.stop } catch { case _ : Throwable => {} }
  }
}
