import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object US_SC_02 {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.driver.memory", "5g")
    .config("spark.driver.maxResultSize", "4g")
    .config("HADOOP_HOME", "C:/hadoop-2.7.1")
    //    .config("spark.executor.memory","9g")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val path = "C:/Users/jflores/Downloads/US_SC_02/"

  val cols = Seq("orgURI","cleanName", "LegalName","vatID" ,"Country", "projURI", "ProjectTitle")




//    .withColumn("cleanName", trim(lower(col("orgName"))))
//    .select(cols.head, cols.tail:_*)








  def printOverlappings(cordis:DataFrame, openaire:DataFrame, scholarly: DataFrame): Unit = {

    println("_____________________________________")
    println("stats preprocessing titles by lowercase and trim")

    println(s"number of distinct titles (using all since there's no linked data keyword) in cordis: ${cordis.select("cleanTitle").distinct().count()}")
    println(s"number of distinct titles (linked data) in scholarly: ${scholarly.select("cleanTitle").distinct().count()}")
    println(s"number of distinct titles (linked data) in openaire: ${openaire.select("cleanTitle").distinct().count()}")
    //    read(cordis, openaire, scholarly);
    val df = cordis.select("cleanTitle").distinct().join(scholarly.select("cleanTitle").distinct(), Seq("cleanTitle"))
    println(s"overlapping titles names in cordis-scholarly ${df.count()}")

    val df2= cordis.select("cleanTitle").distinct().join(openaire.select("cleanTitle").distinct(), Seq("cleanTitle"))
    println(s"overlapping titles names in cordis-openaire ${df2.count()}")

    println("\n_____________________________________")
    println("stats using original titles")

    println(s"number of distinct titles (using all since there's no linked data keyword) in cordis: ${cordis.select("title").distinct().count()}")
    println(s"number of distinct titles (linked data) in scholarly: ${scholarly.select("title").distinct().count()}")
    println(s"number of distinct titles (linked data) in openaire: ${openaire.select("title").distinct().count()}")

    val df1 = cordis.select("title").distinct().join(scholarly.select("title").distinct(), Seq("title"))
    println(s"overlapping titles (no preprocessing) names in cordis-scholarly ${df1.count()}")

    val df22= cordis.select("title").distinct().join(openaire.select("title").distinct(), Seq("title"))
    println(s"overlapping titles (no preprocessing) names in cordis-openaire ${df22.count()}")
  }


  def read(cordis:DataFrame, openaire:DataFrame, scholarly:DataFrame): Unit = {

    cordis.show()
    cordis.printSchema()
    openaire.show()
    openaire.printSchema()

    scholarly.show()
    scholarly.printSchema()
  }

  def getResult(c1:DataFrame, o1:DataFrame, s1: DataFrame): Unit = {

    val part1 = c1.select("year", "projTitle","cleanTitle").distinct().join(s1.select("cleanTitle").distinct(), Seq("cleanTitle"))

    val part2 = c1.select("year", "projTitle","cleanTitle").distinct().join(o1.select("cleanTitle").distinct(), Seq("cleanTitle"))

    val overlap = part1.union(part2)

    val p = c1.select("year", "projTitle","cleanTitle").distinct().groupBy("projTitle", "year").count()
      .select(col("projTitle").as("project"), col("year"), col("count").as("numberOfPublications"))
      .orderBy(desc("year"), desc("numberOfPublications"))

      p.show(false)

    p.repartition(1).write.mode("overwrite").option("header","true")
      .csv(s"${path}result")




  }

  def main(args: Array[String]): Unit = {


    val scholarly = spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("quote","\"").option("escape", "\"").csv(s"${path}scholarly_stem_02.csv")
      .withColumn("title", regexp_replace(col("title"), "\"", "") )
      .withColumn("cleanTitle", trim(lower(col("title"))))

    val cordis = spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("quote","\"").option("escape", "\"")
      .csv(s"${path}cordis_stem_02.csv")
      .withColumn("title", regexp_replace(col("title"), "\"", "") )
      .withColumn("cleanTitle", trim(lower(col("title"))))
//      .withColumn("cleanName", trim(lower(col("LegalName"))))
//      .select(cols.head, cols.tail:_*)

    val openaire = spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("quote","\"").option("escape", "\"")
      .option("multiline", "true").csv(s"${path}OA_query2.4.csv")
      .filter(col("year").isNotNull)
      .withColumn("year2", col("year").cast(IntegerType) )
//      .withColumnRenamed("resTitle", "title")
      .withColumn("title", regexp_replace(col("resTitle"), "\"", "") )
      .withColumn("cleanTitle", trim(lower(col("title"))))
//      .select(cols.head, cols.tail:_*)

//   printOverlappings(cordis, openaire, scholarly)
    getResult(cordis, openaire, scholarly)



    spark.close()
  }

}
