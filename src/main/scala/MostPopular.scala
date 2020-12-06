import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object MostPopular {
  def main(args: Array[String]): Unit = {

    val master = "spark://master:7077"
    val filePath = "hdfs://master:9000//user/root/user_history"

    val conf = new SparkConf().setAppName("MostPopular").setMaster("local[4]")//.setJars(Array("/home/wangchen/Projects/spark2/out/artifacts/spark2_jar/spark2.jar"))
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(filePath)
    val titles: RDD[String] = lines.map(line => line.split("\\|")(5))
    val title_ones: RDD[(String, Int)] = titles.map(title => (title, 1))
    val title_counts: RDD[(String, Int)]  = title_ones.reduceByKey((a,b) => a+b)
    val title_counts_sorted = title_counts.sortBy(item => -item._2)
    val results = title_counts_sorted.take(500)
    for(entry <- results){
      println(entry._1 + ":" + entry._2)
    }
    sc.stop()

  }
}
