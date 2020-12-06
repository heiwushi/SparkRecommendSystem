import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PersonalMostPopularTag {
  def main(args: Array[String]): Unit = {

    val master = "spark://master:7077"
    val filePath = "hdfs://master:9000//user/root/user_history"

    //val conf = new SparkConf().setAppName("spark2").setMaster("local[4]")//.setJars(Array("/home/wangchen/Projects/spark2/out/artifacts/spark2_jar/spark2.jar"))
    val spark = SparkSession.builder.appName("spark2").master("local[4]").getOrCreate()
    val sc = spark.sparkContext
    val lines: RDD[String] = sc.textFile(filePath)
    val uidTagOne: RDD[((String, String), Int)] = lines.flatMap(line =>
      {
        val uid = line.split("\\|")(1)
        val tags:Array[String]  = line.split("\\|")(8).split(",")
        tags.map(t => ((uid, t),1))
      }
    )
    val reduced: RDD[((String, String), Int)] = uidTagOne.reduceByKey((a, b) => a + b)
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)
    val r = grouped.mapValues(item => item.toList.sortBy(-_._2).take(1)).collect()
    for( i <- r){
      println(i)
    }

  }
}
