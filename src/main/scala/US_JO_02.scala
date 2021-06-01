import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object US_JO_02 {

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

  val path = "C:/Users/jflores/Downloads/US_JO_02/"

  val cols = Seq("orgURI","cleanName", "LegalName","vatID" ,"Country", "projURI", "ProjectTitle")




//    .withColumn("cleanName", trim(lower(col("orgName"))))
//    .select(cols.head, cols.tail


  def main(args: Array[String]): Unit = {


    val o1 = spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("quote","\"").option("escape", "\"").csv(s"${path}openaire_jo_02_education.csv")

    val o2 = spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("quote","\"").option("escape", "\"").csv(s"${path}openaire_jo_02_profit.csv")


    val o3 = spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("quote","\"").option("escape", "\"").csv(s"${path}openaire_jo_02_researchorg.csv")

    val openaire = o1.union(o2).union(o3)

    println(s"number of distinct org ${openaire.select("orgURI").distinct().count}")
    println(s"number of distinct result ${openaire.select("resultURI" +
      "").distinct().count}")

    openaire.select( "catLabel", "resultURI").groupBy("catLabel").agg(count("resultURI"))
      .withColumnRenamed("catLabel", "organization type")
      .withColumnRenamed("count(resultURI)", "number of publications")
      .repartition(1).write.mode("overwrite").option("header","true")
      .csv(s"${path}result")

    //    o1.printSchema()
//    o2.printSchema()
//    o3.printSchema()
//    getResult(cordis, openaire, scholarly)



    spark.close()
  }

}
