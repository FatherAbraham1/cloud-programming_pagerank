package cp2016.pagerank.stats

import scala.xml._
import org.apache.spark._
import org.apache.hadoop.fs._

object ParserTimer {
  def main(args: Array[String]) {
    val inputPath = args(0)

    val config = new SparkConf().setAppName("ParserTimer")
    val ctx = new SparkContext(config)

    val pages = ctx.textFile(inputPath, ctx.defaultParallelism * 9)

    val linkPattern = """\[\[[^\]]+\]\]""".r
    val linkSplitPattern = "[#|]"

    val startTime = System.nanoTime()
    val _ = pages.flatMap { line =>
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
    }.collect()

    println(s"parsing time = " + ((System.nanoTime - startTime) / 1000000000.0))
  }
}