package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setAppName("WikiApp").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(line => WikipediaData.parse(line))

  /** Returns the number of articles on which the language `lang` occurs.
    *  Hint1: consider using method `aggregate` on RDD[T].
    *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
    */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = rdd.filter(doc => doc.mentionsLanguage(lang)).count().toInt

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = langs.map(lng => (lng, occurrencesOfLang(lng, rdd))).sortBy(l => l._2).reverse

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langList: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    val rdd1 = rdd.map(w => (w, langList.filter(l => w.mentionsLanguage(l))))
    val rdd2 = rdd1.flatMap( w => {

      val n = w._2.map(l => (l, w._1))

      n
    })
    val rdd3 = rdd2.groupBy(t => t._1)
    val rdd4 = rdd3.map( t => (t._1, t._2.map(w => w._2)))

rdd
    rdd.map(w => (w, langList.filter(l => w.mentionsLanguage(l))))
      .flatMap( w => w._2.map(l => (l, w._1)))
      .groupBy(t => t._1)
      .map( t => (t._1, t._2.map(w => w._2)))

  }

  def getLangRdd(rdd: RDD[String], list: List[RDD[String]]): RDD[String] = {
    if(list.isEmpty) rdd
    else
      getLangRdd(rdd.union(list.head), list.tail)
  }


  /*    rdd.groupBy(x=> {
      val tmp = langs.filter( l => x.mentionsLanguage(l))
      if(tmp == Nil) ""
      else tmp.head
    }).filter(x => x._1!="")*/


  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = index.map(x => (x._1, x._2.size)).collect().toList.sortBy(t => t._2).reverse

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = rankLangsUsingIndex(makeIndex(langs, rdd))

  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
