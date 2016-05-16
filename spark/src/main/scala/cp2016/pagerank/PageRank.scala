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

    val pages = ctx.textFile(inputPath, ctx.defaultParallelism * 9)

    val linkPattern = """\[\[[^\]]+\]\]""".r
    val linkSplitPattern = "[#|]"
    
    val parseStartTime = System.nanoTime()
    println("Start parsing")
    val adjMat = pages.flatMap { line =>
      val xmlElement = XML.loadString(line)
      val title = (xmlElement \\ "title").text.capitalize
      var links = linkPattern.findAllIn(xmlElement.text)
                             .toArray
                             .map { link => link.substring(2, link.length() - 2).split(linkSplitPattern) }
                             .filter { arr => arr.size > 0 }
                             .map { arr => (arr(0).capitalize, title) }

      links.union(Array((title, "🐦" + title + "🐦")))
    }.groupByKey(ctx.defaultParallelism * 9).filter { tup => 
      val magicWord = "🐦" + tup._1 + "🐦"
      val titles = tup._2.toSet
      titles.contains(magicWord)
    }.flatMap { tup =>
       val link = tup._1
       val magicWord = "🐦" + link + "🐦"
       val titles = tup._2.toSet
       titles.map { x =>
         if (x != magicWord) {
           (x, link)
         } else {
           (link, "")
         }
       }
    }.groupByKey(ctx.defaultParallelism * 9).map { tup =>
      if(tup._2.size == 1){
        (tup._1, Iterable())
      } else {
        (tup._1, tup._2.filter(x => !x.isEmpty()))
      }
    }.cache
    
    val finalT = System.nanoTime()
    println("Parsing time " + ((finalT - parseStartTime)/1000000000.0).toString())
    
    val teleport = 0.15
    
    val rank_s = System.nanoTime()
    var ranks = adjMat.map(x => (x._1, 1.0))
    val rank_e = System.nanoTime()
    println("rank map time " + ((rank_e - rank_s)/1000000000.0).toString())
    
    val sinkNodes = adjMat.filter(tup => tup._2.size == 0).cache()
    
    var diff = 0.0
    var iter = 0
    do {
      val begin = System.nanoTime()
      val sinkNodeRankSum = sinkNodes.join(ranks)
                                     .map(tup => tup._2._2)
                                     .sum() * 0.85

      val updates = adjMat.join(ranks)
                          .values
                          .filter(tup => tup._1.size >= 1)
                          .flatMap { case (links, rank) =>
                            val size = links.size
                            links.map(x => (x, rank / size))
                           }
      var newRanks = updates.reduceByKey(_ + _)
      newRanks = ranks.fullOuterJoin(newRanks).map(x => (x._1, x._2._2.getOrElse(0.0) * 0.85 + teleport + sinkNodeRankSum))
      diff = ranks.join(newRanks).map(x => math.abs(x._2._1 - x._2._2)).sum()
      println("diff = " + diff.toString())
      ranks = newRanks

      val end = System.nanoTime()
      println(s"round: " + (end - begin)/1000000000.0)
    } while(diff >= 0.001)

    val numDocs = ranks.count() 
    ranks.sortBy(tup => (-tup._2, tup._1), true, ctx.defaultParallelism * 9)
          .map(tup => tup._1 + "\t" + (tup._2 / numDocs).toString())
          .saveAsTextFile(outputDir)

    try { ctx.stop } catch { case _ : Throwable => {} }
  }
}
