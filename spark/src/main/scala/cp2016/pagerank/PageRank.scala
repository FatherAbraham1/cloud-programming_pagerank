package cp2016.pagerank

import scala.xml._
import org.apache.spark._
import org.apache.hadoop.fs._

object PageRank {
  def unescape(str: String) : String = {
    str.replaceAllLiterally("&lt;", "<")
       .replaceAllLiterally("&gt;", ">")
       .replaceAllLiterally("&amp;", "&")
       .replaceAllLiterally("&quot;", "\"")
       .replaceAllLiterally("&apos;", "'")
  }
  
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

    val pages = ctx.textFile(inputPath, ctx.defaultParallelism * 3)

    val linkPattern = """\[\[[^\]]+\]\]""".r
    val linkSplitPattern = "[#|]"
    var adjMatrix = pages.flatMap { line =>
      val xmlElement = XML.loadString(line)
      val title = (xmlElement \\ "title").text.capitalize
      var links = linkPattern.findAllIn(line)
                             .toArray
                             .map { link => link.substring(2, link.length() - 2).split(linkSplitPattern) }
                             .filter { arr => arr.size > 0 }
                             .map { arr => (title, unescape(arr(0)).capitalize) }

      links.union(Array((title, "")))
    }

    val numDocs = pages.count()
    val teleport = 0.15 * (1.0 / numDocs)
    
    adjMatrix = adjMatrix.map(x => (x._2, x._1))
                         .leftOuterJoin(adjMatrix, ctx.defaultParallelism * 12)
                         .filter(x => x._1 == "" || !x._2._2.isEmpty)
                         .filter(x => !x._2._2.exists(e => e == ""))
                         .map(x => (x._2._1, x._1))

    val adjMat = adjMatrix.groupByKey().cache()
    var ranks = adjMat.map(x => (x._1, 1.0 / numDocs))

    var diff = 0.0
    var iter = 0
    do {
      val begin = System.nanoTime()
      var sinkNodeRankSum = adjMat.join(ranks)
                                  .filter(tup => tup._2._1.size == 1)
                                  .map(tup => tup._2._2)
                                  .sum()

      sinkNodeRankSum = sinkNodeRankSum / numDocs * 0.85


      val updates = adjMat.join(ranks)
                          .values
                          .filter(tup => tup._1.size > 1)
                          .flatMap { case (links, rank) =>
                            val size = links.size
                            links.filter(x => x != "")
                                 .map(x => (x, rank / (size - 1)))
                           }
      var newRanks = updates.reduceByKey(_ + _)
      newRanks = ranks.fullOuterJoin(newRanks).map(x => (x._1, x._2._2.getOrElse(0.0) * 0.85 + teleport + sinkNodeRankSum))
      diff = ranks.join(newRanks).map(x => math.abs(x._2._1 - x._2._2)).sum()
      println("diff = " + diff.toString())
      ranks = newRanks

      val end = System.nanoTime()
      println(end - begin)
    } while(diff >= 0.001)

    ranks.sortBy(tup => (-tup._2, tup._1), true, ctx.defaultParallelism * 12)
          .map(tup => tup._1 + "\t" + tup._2.toString())
          .saveAsTextFile(outputDir)

    try { ctx.stop } catch { case _ : Throwable => {} }
  }
}
