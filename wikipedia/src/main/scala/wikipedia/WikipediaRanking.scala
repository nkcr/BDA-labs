package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String)

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy").map(_.toLowerCase)

  val conf: SparkConf = new SparkConf().setAppName("WikipediaRanking").setMaster("local[20]")
  val sc: SparkContext = new SparkContext(conf)
  val wikiRdd: RDD[WikipediaArticle] = sc.parallelize(WikipediaData.articles).filter(_.text.toLowerCase.split(" ").count(langs contains _) > 0).map(oldW => WikipediaArticle(oldW.title,oldW.text.toLowerCase()))

  /** Returns the number of articles on which the language `lang` occurs.
    * Hint1: consider using method `aggregate` on RDD[T].
    * Hint2: should you count the "Java" language when you see "JavaScript"?
    * Hint3: the only whitespaces are blanks " "
    * Hint4: no need to search in the title :)
    */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = rdd.aggregate(0)((acc, value) => if (value.text.split(" ") contains lang) acc + 1 else acc, (acc1, acc2) => acc1 + acc2)

  //def occurrencesOfLangForIndex(lang: String, rdd: RDD[WikipediaArticle]): Iterable[WikipediaArticle] = rdd.aggregate(Set[WikipediaArticle]())((acc, value) => if (value.text.toLowerCase().split(" ") contains lang.toLowerCase()) acc + value else acc, (acc1, acc2) => acc1.union(acc2))

  def occurrencesOfLangForIndex(lang: String, rdd: RDD[WikipediaArticle]): RDD[(String, WikipediaArticle)] = {
    rdd.filter(article => article.text.split(" ") contains lang).map((lang, _))
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = langs.map(l => (l, occurrencesOfLang(l, rdd))).filter(_._2 > 0).sortBy(-_._2)

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] =  langs.map( l => occurrencesOfLangForIndex(l,rdd)).reduce(_ union _).groupByKey

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = index.map((i) => (i._1, i._2.size)).collect().toList.sortBy(-_._2)

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking is combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = rdd.flatMap( _.text.split(" ").distinct).map((_,1)).reduceByKey(_+_).filter(langs contains _._1 ).collect().toList.sortBy(-_._2)

  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))
    println(langsRanked)

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)


    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))
    println(langsRanked2)

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))
    println(langsRanked3)

    /* Output the speed of each ranking */
    println(timing)
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
