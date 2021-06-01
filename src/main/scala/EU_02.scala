import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object EU_02 {

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

  val path = "C:/Users/jflores/Downloads/EU_02/"

  val cols = Seq("orgURI","cleanName", "LegalName","vatID" ,"Country", "projURI", "ProjectTitle")




//    .withColumn("cleanName", trim(lower(col("orgName"))))
//    .select(cols.head, cols.tail

  def stats(cordis:DataFrame, european: DataFrame): Unit = {
    val distP = cordis.select("proj").distinct()
    val distEUP = european.select("proj").distinct()

    val overlap = cordis.select(lower(trim(col("projTitle")) ).as("projTitle")  ).distinct()
      .join(european.select( lower(trim(col("projTitle")) ).as("projTitle")  ).distinct() , Seq("projTitle")  ).count()

    println(s"number cordis project ${distP.count()}")
    println(s"number european project ${distEUP.count()}")
    println(s"number overlapping project ${overlap}")

    val distOP = cordis.select("pic").distinct()
    val distOEUP = european.select("pic").distinct()

    val overlapO = cordis.select("pic").distinct()
      .join(european.select( "pic" ).distinct() , Seq("pic")  ).count()


    println(s"number cordis org ${distOP.count()}")
    println(s"number european org ${distOEUP.count()}")
    println(s"number overlapping org ${overlapO}")



  }

  def getResult(cordis:DataFrame, european: DataFrame): Unit = {

    val orgNames = cordis.select("pic","orgName" )
      .union(european.select("pic", "orgName"))
      .distinct()
      .groupBy("pic")
      .agg(concat_ws("_*_",collect_set("orgName")).as("orgName"))
      .withColumn("organizationName", functions.split(col("orgName"), "_*_").getItem(0))
      .drop("orgName").distinct()

    val df =  cordis.select( "region", "source","budget", "proj").union(european.select( "region", "source","budget", "proj"))
      .groupBy("region", "source")
      .agg(count("proj").as("numberProjects"),  sum("budget").as("totalBudget"))
      .select("region","source", "totalBudget", "numberProjects")
      .orderBy(desc("totalBudget"),desc("numberProjects"))



    df.repartition(1).write.mode("overwrite").option("header","true")
      .csv(s"${path}result")


  }

  def main(args: Array[String]): Unit = {


    val cordis = spark.read
      .option("header", "true").option("multiline", "true")
      .option("quote","\"").option("escape", "\"").csv(s"${path}eu_cordis_02.csv")
      .withColumn("budget", col("budget").cast(DoubleType))
      .withColumn("source", lit("CORDIS"))
      .withColumnRenamed("city", "region")


    val european = spark.read
      .option("header", "true")
      .option("quote","\"").option("escape", "\"").csv(s"${path}eu_european_02.csv")
      .withColumn("budget", col("budget").cast(DoubleType))
      .withColumnRenamed("projName", "projTitle")
      .withColumn("source", lit("kohesio"))

    cordis.printSchema()
    european.printSchema()

//    cordis.filter(col("beneficiaryName") === "998463002" ).show(false)
//    european.filter(col("beneficiaryName") === "998463002" ).show()

    getResult(cordis, european)
    stats(cordis, european)

    /*
      */

    //    o1.printSchema()
//    o2.printSchema()
//    o3.printSchema()
//    getResult(cordis, openaire, scholarly)



    spark.close()
  }

}
