import US_RO_01.mapDS
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_set, concat_ws, desc, lit, lower, size, split, trim, udf, when}

object US_JO_01 {

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
    .csv(s"${path}cordis3_new.csv")
    .withColumn("cleanName", trim(lower(col("orgName"))))
    .select(cols.head, cols.tail:_*)

  val cordis_acronym = spark.read
    .option("header", "true").option("inferSchema", "true")
    .option("quote","\"").option("escape", "\"")
    .option("multiline", "true")
    .csv(s"${path}cordis3_new.csv")
    .withColumn("cleanName", trim(lower(col("orgName"))))
    .select("acronym", cols:_*)

  val openaire_ah = spark.read
    .option("header", "true").option("inferSchema", "true")
    .option("quote","\"").option("escape", "\"").csv(s"${path}openaire_ah.csv")
    .withColumn("cleanName", trim(lower(col("orgName"))))
    .select(cols.head, cols.tail:_*)

  val openaire_iq = spark.read
    .option("header", "true").option("inferSchema", "true")
    .option("quote","\"").option("escape", "\"").csv(s"${path}openaire_iq.csv")
    .withColumn("cleanName", trim(lower(col("orgName"))))
    .select(cols.head, cols.tail:_*)

  val openaire_rz = spark.read
    .option("header", "true").option("inferSchema", "true")
    .option("quote","\"").option("escape", "\"").csv(s"${path}openaire_rz.csv")
    .withColumn("cleanName", trim(lower(col("orgName"))))
    .select(cols.head, cols.tail:_*)

  val openaire_subjects = spark.read
    .option("header", "true").option("inferSchema", "true")
    .option("quote","\"").option("escape", "\"").csv(s"${path}openaire3_subjects.csv")
    .withColumn("cleanName", trim(lower(col("orgName"))))
    .select("acronym", cols:_*)

//  val op = openaire_subjects.select(col("orgURI"),col("projectURI"), lower(col("acronym")).as("acronym"))
//  val cor = cordis_acronym.select(col("orgURI"),col("projectURI"), lower(col("acronym")).as("acronym"))
//  val open = op.join(cor,Seq("acronym"),"left")

  var openaire = openaire_ah.union(openaire_iq).union(openaire_rz)


  val dbpedia = spark.read
    .option("header", "true").option("inferSchema", "true")
    .option("quote","\"").option("escape", "\"").csv(s"${path}dbpedia_country_hdi_new.csv")
    .withColumnRenamed("country", "countryName")
    .withColumnRenamed("countryURI", "country")

  val setURIUDF = udf(setURI(_:String, _:String): String)
  def stats(): Unit = {

//    dbpedia.show
    val df = cordis.select("cleanName").distinct.join(openaire.select("cleanName")
      .union(openaire_subjects.select("cleanName")).distinct, Seq("cleanName"))
    println(s"number of unique org in cordis: ${cordis.select("cleanName").distinct.count}")
    println(s"number of unique org in openaire: ${openaire.select("cleanName")
      .union(openaire_subjects.select("projectURI")).distinct.count}")
    println("overlapping cordis-openaire: " +df.count())

    val cnt = openaire_subjects.select("cleanName", "projectURI").join(df, Seq("cleanName"))
      .select("projectURI").distinct().count
println(s"javi new projects added ${cnt}")
    val titles = cordis.select(trim(lower(col("title"))).as("title"))
      .join(openaire.select(trim(lower(col("title"))).as("title")), Seq("title"))

    val titles2 = cordis.select(trim(lower(col("title"))).as("title"))
      .distinct().join(openaire_subjects.select(trim(lower(col("title"))).as("title")).distinct(), Seq("title"))
    println(s"number of unique titles in cordis: ${cordis.select("projectURI").distinct().count}")
    println(s"number of unique titles in openaire: ${
      openaire.select("title").union(openaire_subjects.select("title"))
        .distinct().count}")
    println(s"number of unique titles in openaire jus tsubejects: ${
      openaire_subjects.select("title")
        .distinct().count}")


    println("overlapping cordis-openaire usin title str: " +titles.count() +"- "+ titles2.count())
    println(s"***number of unique titles in cordis: ${cordis_acronym.select("projectURI").distinct().count}")
    println(s"cordis acronym ${cordis_acronym.select("acronym").distinct().count()}")
    println(s"openaire acronym ${openaire_subjects.select("acronym").distinct().count()}")
    println(s"overlalp: ${cordis_acronym.select(lower(col("acronym")).as("acronym")).distinct().join(openaire_subjects.select(lower(col("acronym")).as("acronym")).distinct(), Seq("acronym")).count}")

    val df1 = openaire_subjects.withColumn("acronym",lower(col("acronym"))).distinct()
      .join(cordis_acronym.select(lower(col("acronym")).as("acronym") , col("orgURI").as("orgURIcordis"))
        .distinct(), Seq("acronym"), "anti")

    val df2 = openaire_subjects
      .withColumn("acronym",lower(col("acronym"))).distinct()
      .join(cordis_acronym
        .select(lower(col("acronym")).as("acronym") ,
          col("orgURI").as("orgURIcordis"),
          col("projectURI").as("projectCordis"),
          col("title").as("titleCordis")
        )
        .distinct(), Seq("acronym"),"left")



    mapDS = Map[String, String]()
    df2.select("orgURI", "orgURIcordis").collect().foreach{
      case Row(org1: String, org2: String) =>
        mapDS = mapDS + (org1 -> org2)
    }
//
/*
   val df3 = df2.withColumn("orgURI",
      setURIUDF(col("orgURI"), col("orgURIcordis")))
      .withColumn("projectURI",
        when(col("title") === col("titleCordis"),
          col("projectCordis")).otherwise(col("projectURI")))
      .withColumn("modify",
        when(col("title") === col("titleCordis"),
          lit(1)).otherwise(lit(0)))
*/
//val df4 = df2.withColumn("newORG",
//  setURIUDF(col("orgURI"), col("orgURIcordis")))
//  .withColumn("modify",
//    when(col("newORG").contains("data.europa")   )   )

//    openaire = openaire.union(df3.select(cols.head, cols.tail:_*))
//    df3.repartition(1).write.mode("overwrite").option("header","true")
//      .csv(s"${path}tmp.csv")
//
//   println("*** modify "+ df3.filter(col("modify") === 1).count())
//    println("*** modify2 "+
//      df3.filter(col("orgURI").contains("data.europa") and col("projectURI").contains("openaire")).count())
//println(df3.filter(col("orgURI").contains("data.europa") and col("projectURI").contains("openaire")).select("projectURI").distinct().count)



//    df1.show()
//    df1.printSchema()
//
//    println(df1.count())

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

  def setURI(org1:String, org2:String): String = {
    if(mapDS.contains(org1)){
      println("change")
      return org2
    }
    org1
  }
  def getResults2(): Unit = {
    dbpedia.show
    val all = openaire.union(cordis)

    val o1 = openaire.select("orgURI","country","projectURI", "title")
      .groupBy("orgURI", "country", "projectURI")
      .agg(concat_ws("$",collect_set("title")).as("title")) //to get 1
      .join(dbpedia.select("country", "hdi", "countryName") ,Seq("country"),"left")

    val c2= cordis.select("orgURI","country","projectURI", "title")
      .groupBy("orgURI", "country", "projectURI")
      .agg(concat_ws("$",collect_set("title")).as("title")) //to get 1
      .withColumnRenamed("country","countryName").join(dbpedia.select( col("hdi"),
        col("countryName") ),Seq("countryName"),"left")




    val result = o1.select("orgURI","projectURI","hdi", "countryName")
      .union(c2.select("orgURI","projectURI","hdi", "countryName"))
      .filter(col("hdi").isNotNull)
      .groupBy("orgURI","hdi", "countryName")
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
      .csv(s"${path}US_JO_01_new.csv")


  }

  def getResults (): Unit = {

    val all = openaire.union(cordis)

    val o1 = openaire.select("vatID","country","projectURI", "title")
      .groupBy("vatID", "country", "projectURI")
      .agg(concat_ws("$",collect_set("title")).as("title")) //to get 1
      .join(dbpedia.select("country", "hdi") ,Seq("country"),"left")

    val c2= cordis.select("vatID","country","projectURI", "title")
      .groupBy("vatID", "country", "projectURI")
      .agg(concat_ws("$",collect_set("title")).as("title")) //to get 1
      .join(dbpedia.select("iso", "hdi") ,col("country") === col("iso"),"left")

    o1.printSchema()
    c2.printSchema()

    cordis.filter(col("vatID") === "ZA4140125313").show(5)
    openaire.filter(col("vatID") === "ZA4140125313").show(5)

    o1.select("vatID","projectURI","hdi").union(c2.select("vatID","projectURI","hdi"))
      .filter(col("hdi").isNotNull)
      .groupBy("vatID","hdi")
      .count
      .orderBy(desc("vatID"))
      .show(40)


  }

  def main(args: Array[String]): Unit = {
//    getResults()
    stats()
//    getResults2() //1425

  }


}
