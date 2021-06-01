import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_set, concat_ws, desc, lower, size, split, trim}

object US_PM_01 {

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

  val path = "C:/Users/jflores/Downloads/"
  val cols = Seq("projectURI","title", "orgURI", "orgName", "country", "cleanName")

  val cordis = spark.read
    .option("header", "true").option("inferSchema", "true")
    .option("quote","\"").option("escape", "\"")
    .option("multiline", "true")
    .csv(s"${path}cordis4.csv")
    .withColumn("cleanName", trim(lower(col("orgName"))))
    .select(cols.head, cols.tail:_*)

  val cordis_acronym = spark.read
    .option("header", "true").option("inferSchema", "true")
    .option("quote","\"").option("escape", "\"")
    .option("multiline", "true")
    .csv(s"${path}cordis4_acronym.csv")
    .withColumn("cleanName", trim(lower(col("orgName"))))


  val openaire = spark.read
    .option("header", "true").option("inferSchema", "true")
    .option("quote","\"").option("escape", "\"").csv(s"${path}openaire4.csv")
    .withColumn("cleanName", trim(lower(col("orgName"))))
    .select(cols.head, cols.tail:_*)

  val openaire_subject = spark.read
    .option("header", "true").option("inferSchema", "true")
    .option("quote","\"").option("escape", "\"").csv(s"${path}openaire4_subject.csv")
    .withColumn("cleanName", trim(lower(col("orgName"))))



  val dbpedia = spark.read
    .option("header", "true").option("inferSchema", "true")
    .option("quote","\"").option("escape", "\"").csv(s"${path}dbpedia_country_gdp.csv")
    .withColumnRenamed("country", "countryName")
    .withColumnRenamed("countryURI", "country")
    .filter(col("gdp").isNotNull)

  def stats(): Unit = {

    dbpedia.show
    val df = cordis.select("cleanName").distinct.join(openaire.select("cleanName").distinct, Seq("cleanName"))
    println(s"number of unique org in cordis: ${cordis.select("cleanName").distinct.count}")
    println(s"number of unique org in openaire: ${openaire.select("cleanName").union(
      openaire_subject.select("cleanName")).distinct.count}")
    println("overlapping cordis-openaire: " +df.count())

    val titles = cordis.join(openaire, Seq("title"))
    val titles2 = cordis.select(trim(lower(col("title"))).as("title"))
      .distinct().join(openaire_subject.select(trim(lower(col("title"))).as("title")).distinct(), Seq("title"))

    cordis.select(trim(lower(col("title"))).as("title"))
      .distinct().join(openaire_subject.select(trim(lower(col("title"))).as("title"), col("cleanName")).distinct(), Seq("title"))
      .select("cleanName").distinct()
      .show(50, false)


    cordis.select(trim(lower(col("title"))).as("title"), col("cleanName"))
      .distinct().join(openaire_subject.select(trim(lower(col("title"))).as("title")).distinct(), Seq("title"))
      .select("cleanName").distinct()
      .show(50, false
      )


    println(s"number of unique titles in cordis: ${cordis.select("projectURI").distinct().count}")
    println(s"number of unique titles in openaire: ${openaire.select("projectURI").distinct().
      union(
        openaire_subject.select("projectURI")).distinct().count}")
    println("overlapping cordis-openaire: " +titles.count()  +"- "+ titles2.count())
    println(s"number of unique titles in openaire jus tsubejects: ${
      openaire_subject.select("title")
        .distinct().count}")


    println(s"cordis acronym ${cordis_acronym.select("acronym").distinct().count()}")
    println(s"openaire acronym ${openaire_subject.select("acronym").distinct().count()}")
    println(s"overlalp: ${cordis_acronym.select(lower(col("acronym")).as("acronym")).distinct().join(openaire_subject.select(lower(col("acronym")).as("acronym")).distinct(), Seq("acronym")).count}")

    //    val df = cordis.select("vatID").distinct.join(openaire.select("vatID").distinct, Seq("vatID"))
//    println(s"number of unique org in cordis: ${cordis.select("vatID").distinct.count}")
//    println(s"number of unique org in openaire: ${openaire.select("vatID").distinct.count}")
//    println("overlapping cordis-openaire: " +df.count())
//
//    val titles = cordis.join(openaire, Seq("title"))
//    println(s"number of unique titles in cordis: ${cordis.select("title").count}")
//    println(s"number of unique titles in openaire: ${openaire.select("title").count}")
//    println("overlapping cordis-openaire: " +titles.count())
//    df.show()

//    cordis.select("cleanName", "orgURI").groupBy("cleanName").count.orderBy(desc("count")).show

    //      .withColumn("joins", size(split(col("orgs"), ",")))
//      .filter(col("joins")>2).show(false)



//    cordis.filter(col("cleanName") === "it").show(false)
//    openaire.select("orgURI","cleanName").groupBy("cleanName")
//      .agg(concat_ws(",",collect_set("orgURI")).as("orgs"))
//      .withColumn("joins", size(split(col("orgs"), ",")))
//      .filter(col("joins")>2).show(false)
//    +------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----+
//    |cleanName         |orgs                                                                                                                                                                                                                                  |joins|
//    +------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----+
//    |ministry of health|http://data.europa.eu/s66/Organisations#2795047d-3004-33c2-8730-8e69a36c9cfc,http://data.europa.eu/s66/Organisations#9c69c34f-a0fe-34c1-aac4-39389fef63bb,http://data.europa.eu/s66/Organisations#45f5d834-f55d-3acf-bf92-614d4111d098|3    |
//    +------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----+





  }

  def getResults2(): Unit = {
    val all = openaire.union(cordis)

    val o1 = openaire.select("orgURI","country","projectURI", "title")
      .groupBy("orgURI", "country", "projectURI")
      .agg(concat_ws("$",collect_set("title")).as("title")) //to get 1
      .join(dbpedia.select("country", "gdp", "countryName") ,Seq("country"),"left")

    val c2= cordis.select("orgURI","country","projectURI", "title")
      .groupBy("orgURI", "country", "projectURI")
      .agg(concat_ws("$",collect_set("title")).as("title")) //to get 1
      .join(dbpedia.select("iso", "gdp", "countryName") ,col("country") === col("iso"),"left")

    val result = o1.select("orgURI","projectURI","gdp", "countryName")
      .union(c2.select("orgURI","projectURI","gdp", "countryName"))
      .filter(col("gdp").isNotNull)
      .groupBy("orgURI","gdp", "countryName")
      .count
      .orderBy(desc("count"))



    val namesO = openaire.select("orgURI","orgName")
      .groupBy("orgURI")
      .agg(concat_ws("$",collect_set("orgName")).as("names")) //to get 1
      .select(col("orgURI"), split(col("names"), "$").getItem(0).as("orgName"))

    val namesC= cordis.select("orgURI","orgName")
      .groupBy("orgURI")
      .agg(concat_ws("$",collect_set("orgName")).as("names"))
      .select(col("orgURI"), split(col("names"), "$").getItem(0).as("orgName"))

    val names = namesO.union(namesC)

    val resultFinal = result.join(names, Seq("orgURI")).drop("orgURI")
      .orderBy(desc("count"))

    resultFinal.show(70,false)
    println(resultFinal.count())
    resultFinal.repartition(1).write.mode("overwrite").option("header","true")
      .csv(s"${path}US_PM_01.csv")


  }

  def getResults (): Unit = {

    val all = openaire.union(cordis)

    val o1 = openaire.select("vatID","country","projectURI", "title")
      .groupBy("vatID", "country", "projectURI")
      .agg(concat_ws("$",collect_set("title")).as("title")) //to get 1
      .join(dbpedia.select("country", "gdp") ,Seq("country"),"left")

    val c2= cordis.select("vatID","country","projectURI", "title")
      .groupBy("vatID", "country", "projectURI")
      .agg(concat_ws("$",collect_set("title")).as("title")) //to get 1
      .join(dbpedia.select("iso", "gdp") ,col("country") === col("iso"),"left")

    o1.printSchema()
    c2.printSchema()

    cordis.filter(col("vatID") === "ZA4140125313").show(5)
    openaire.filter(col("vatID") === "ZA4140125313").show(5)

    o1.select("vatID","projectURI","gdp").union(c2.select("vatID","projectURI","gdp"))
      .filter(col("gdp").isNotNull)
      .groupBy("vatID","gdp")
      .count
      .orderBy(desc("vatID"))
      .show(40)


  }

  def main(args: Array[String]): Unit = {
//    getResults()
//    getResults2() //1425
   stats()
  }


}
