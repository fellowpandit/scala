import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.rdd
object WordCount{
    def main(args: Array[String]) {
        val pathToFile = "D:/SPARK/WordCount.sbt"
        val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
        val sc = new  SparkContext(conf)
        val wordsRDD = sc.textFile(pathToFile).flatMap(_.split(" "))
        val wordCountInitRdd = words.RDD.map(word => (word,1))

        val wordCountRdd = wordCountInitRdd.reduceByKey((v1,v2) => v1+v2)
        val highFreqWords = wordCountRdd.filter(x => x._2 > 4)
        highFreqWords.saveAsTextFile("wordcountsDir")
    }
}
