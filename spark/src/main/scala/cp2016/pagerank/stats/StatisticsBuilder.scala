package cp2016.pagerank.stats

import scala.xml._
import org.apache.spark._
import org.apache.hadoop.fs._

object StatisticsBuilder {
  def main(args: Array[String]) {
    val inputPath = args(0)

    val config = new SparkConf().setAppName("Stats")
    val ctx = new SparkContext(config)

    val pages = ctx.textFile(inputPath, ctx.defaultParallelism * 9)

    val linkPattern = """\[\[[^\]]+\]\]""".r
    val linkSplitPattern = "[#|]"

    var adjMat = pages.flatMap { line =>
      val xmlElement = XML.loadString(line)
      val title = (xmlElement \\ "title").text.capitalize
      var links = linkPattern.findAllIn(xmlElement.text)
                             .toArray
                             .map { link => link.substring(2, link.length() - 2).split(linkSplitPattern) }
                             .filter { arr => arr.size > 0 }
                             .map { arr => (arr(0).capitalize, title) }

      links.union(Array((title, "🐦" + title + "🐦")))
    }
    
    println(s"total links = " + adjMat.count)
    
    
    val aa = adjMat.groupByKey
    
    val invalid = aa.filter { tup => 
      val magicWord = "🐦" + tup._1 + "🐦"
      val titles = tup._2.toSet
      !titles.contains(magicWord)
    }.map(x => x._2.size).sum
    
    println(s"invalid links = " + invalid)

    println(s"numDocs = " + pages.count)
  }
}