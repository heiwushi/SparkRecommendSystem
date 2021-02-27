import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FastItemCF {
  def main(args: Array[String]): Unit = {
    val master = "spark://master:7077"
    val filePath = "hdfs://master:9000//user/root/user_history"

    val conf: SparkConf = new SparkConf().setAppName("ItemCF").setMaster("local[4]") //.setJars(Array("/home/wangchen/Projects/spark2/out/artifacts/spark2_jar/spark2.jar"))
    val sc: SparkContext = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(filePath)

    //用户点击记录
    val userClickItem: RDD[(String, String)] = lines.map(line => {
      val fields: Array[String] = line.split("\\|")
      (fields(1), fields(5))
    }).distinct()
    //将每个用户的点击记录聚合起来
    var userClickItems: RDD[(String, Iterable[String])] = userClickItem.groupByKey()
    //过滤掉只有一次点击的用户，这样的用户无法产生两两物品的共现
    userClickItems = userClickItems.filter(x => {x._2.size > 1})
    //凡是两两共现过一次的，就置为1
    val itemItemCooccurenceOne: RDD[((String, String), Int)] = userClickItems.flatMap(x => for {item_a <- x._2; item_b <- x._2 if !item_a.equals(item_b)} yield ((item_a, item_b), 1))
    //按key相加后得到两两物品共现的次数
    val itemItemCooccurenceNum: RDD[((String, String), Int)] = itemItemCooccurenceOne.reduceByKey((x, y) => x + y)

    //物品被用户点击的记录
    val itemClickByUser = lines.map(line => {
      val fields: Array[String] = line.split("\\|")
      (fields(5), fields(1))
    }).distinct()
    //物品点击数
    val itemClickNum: RDD[(String, Int)] = itemClickByUser.map(x => (x._1, 1)).reduceByKey(_ + _)
    /*
    到目前，我们已经获得了每个物品的被点击次数和两两物品之间的共现次数
    根据公式similarity(i,j) = |N(i) 𝉅 N(j)|/ sqrt(|N(i)|*|N(j)|)
    需要将itemItemCooccurenceNum和itemClickNum分别以为两个物品id为连接key做两次join，得到所有需要的值
    从而可以计算相似度了
     */

    val itemItemCooccurenceNumTmp: RDD[(String, (String, Int))] = itemItemCooccurenceNum.map {
      case ((x, y), clicks) =>
        (x, (y, clicks))
    }
    //第一次join
    val joinTmp1: RDD[(String, ((String, Int), Int))] = itemItemCooccurenceNumTmp
      .join(itemClickNum)

    val joinTmp1map: RDD[(String, (Int, String, Int))] = joinTmp1.map {
      case (x, ((y, clicks), xClickCnt)) =>
        (y, (clicks, x, xClickCnt))
    }
    //第二次join
    val joinTmp2: RDD[(String, ((Int, String, Int), Int))] = joinTmp1map.join(itemClickNum)
    val itemItemSimilarity: RDD[(String, String, Double, String)] = joinTmp2.map {
      case (y, ((clicks, x, xClickCnt), yClickCnt)) =>
        {

          val cosine = clicks / math.sqrt(xClickCnt * yClickCnt)
          (x, y,cosine, clicks.toString+" "+xClickCnt.toString +" "+ yClickCnt.toString)
//          if (x.equals(y)){
//            (x, y, -1.0)
//          }else{
//            val cosine = clicks / math.sqrt(xClickCnt * yClickCnt)
//            (x, y, cosine)
//          }
        }
    }
    val itemItemSimilarityIter: RDD[(String, Iterable[(String, String, Double, String)])] = itemItemSimilarity.groupBy(x => x._1)
    itemItemSimilarityIter.mapValues(
      x=>x.toList.map(y => (y._2,y._3,y._4)).sortBy(z=> -z._2).take(10)
    ).foreachPartition((partition: Iterator[(String, List[(String, Double, String)])]) => {
      for(r: (String, List[(String, Double, String)]) <- partition){
        println(r)
      }

    })


  }

}
