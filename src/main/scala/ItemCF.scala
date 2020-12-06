import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object ItemCF {
  def main(args: Array[String]): Unit = {
    val master = "spark://master:7077"
    val filePath = "hdfs://master:9000//user/root/user_history"

    val conf: SparkConf = new SparkConf().setAppName("ItemCF").setMaster("local[4]")//.setJars(Array("/home/wangchen/Projects/spark2/out/artifacts/spark2_jar/spark2.jar"))
    val sc: SparkContext = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(filePath)

    val user_item: RDD[(String, String)] = lines.map(line => {
      val fields: Array[String] = line.split("\\|")
      (fields(1), fields(5))
    })

    val user_items: RDD[(String, Iterable[String])] = user_item.groupByKey()

    val item_user = lines.map(line => {
      val fields: Array[String] = line.split("\\|")
      (fields(5), fields(1))
    })

    val item_users: RDD[(String, Iterable[String])] = item_user.groupByKey()
    val item_cartesian: RDD[((String, Iterable[String]), (String, Iterable[String]))] = item_users.cartesian(item_users)
    item_cartesian.foreach(item => println(item))
    sc.stop()
  }


  def similar(x: ((String, Iterable[String]), (String, Iterable[String]))): Unit ={


  }
}
