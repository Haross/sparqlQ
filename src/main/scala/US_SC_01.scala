import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object US_SC_01 {

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

  val path = "C:/Users/jflores/Downloads/US_SC_01/"

  val cols = Seq("orgURI","cleanName", "LegalName","vatID" ,"Country", "projURI", "ProjectTitle")




//    .withColumn("cleanName", trim(lower(col("orgName"))))
//    .select(cols.head, cols.tail:_*)


  def stats(cordis:DataFrame, openaire:DataFrame): Unit = {
    printDefaulStat(cordis, "cordis")
    printDefaulStat(openaire, "openarie")

    printOverlappings(cordis, openaire, "cordis", "openaire")
  }

  def printDefaulStat(df: DataFrame, name:String) : Unit = {
    val numberOrg = df.select("orgURI").distinct().count()
    val numberOfProjects = df.select("projURI").distinct().count()

    println(s"number of unique organizations (URI) for ${name} is ${numberOrg}")
    println(s"number of unique projects for (URI) ${name} is ${numberOfProjects}")


  }

  def mergeOrgByVatID(df1:DataFrame, df2:DataFrame): DataFrame = {

//    println("-----------------------------------------")
    val df1_q = df1.select(col("vatID"),lower(col("ProjectTitle")).as("ProjectTitle") )
    val df2_q = df2.select(col("vatID"), lower(col("ProjectTitle")).as("ProjectTitle") )

    val merge = df1_q.union(df2_q).distinct()

    val df1_count = df1_q.groupBy("vatID").count().withColumnRenamed("count","countByCORDIS")
    val merge_count = merge.groupBy("vatID").count().withColumnRenamed("count","countByMerge")

    val merge2 = df1_count.join(merge_count, "vatID")
      .withColumn("newProjects", col("countByMerge")- col("countByCORDIS") )
      .filter(col("newProjects") > 0)

    merge2.show
    println("new projects added:")
    merge2.agg(sum("newProjects")).show()


    val df1_country = df1.select(col("vatID"), lower(col("ProjectTitle")).as("ProjectTitle"))
    val df2_country = df2.select(col("vatID"), lower(col("ProjectTitle")).as("ProjectTitle"))

    merge

  }



  def printOverlappings(df1:DataFrame, df2:DataFrame, name1: String, name2: String): Unit = {

    val overlappingOrgNames = df1.select("legalName").distinct().join(df2.select("legalName").distinct(), "legalName").count
    println(s"Overlapping org names for ${name1} and ${name2} is $overlappingOrgNames ")

    val overlappingVAT = df1.select(col("vatID")).distinct().join(df2.select("vatID")
      .distinct(), "vatID").count
      //df1.select(upper(col("vatID")).as("vatID")).distinct().join(df2.select(upper(col("vatID")).as("vatID"))
      //.distinct(), "vatID").count
    println(s"Overlapping org using vatID for ${name1} and ${name2} is $overlappingVAT ")

    val overlappingTitles = df1.select(lower(col("ProjectTitle")).as("ProjectTitle")).distinct()
      .join(df2.select(lower(col("ProjectTitle")).as("ProjectTitle")).distinct(), "ProjectTitle").count

    println(s"Overlapping titles for ${name1} and ${name2} is $overlappingTitles ")



  }


  def read(cordis:DataFrame, openaire:DataFrame, dbpedia:DataFrame): Unit = {
    cordis.show()

    openaire.show(100)
    openaire.printSchema()

    dbpedia.show()
  }

  def getResult(c1:DataFrame, o1:DataFrame): Unit = {
// for checking if a project has multiple titles
//    openaire.select("vatID","projURI", "ProjectTitle")
//      .groupBy("vatID", "projURI")
//      .agg(concat_ws("$-$",collect_set("ProjectTitle")).as("array") )
//      .withColumn("titlesDuplicated", size(split(col("array"),"$-$")))
//      .orderBy(desc("titlesDuplicated")).show

    val cordis = c1.withColumn("vatID",when(col("vatID").isNull || lower(col("vatID")).isin(Seq("nan","missing"):_*)
      , col("orgURI")).otherwise(col("vatID")))
    val openaire = o1.withColumn("vatID",when(col("vatID").isNull || lower(col("vatID")).isin(Seq("nan","missing"):_*)
      , col("orgURI")).otherwise(col("vatID")))

    val merge = mergeOrgByVatID(cordis,openaire)
//      merge.repartition(1).write.mode("overwrite").option("header","true")
//      .csv(s"${path}US_PM_01.csv")
//

    val countryInfo = cordis.select("Country","vatID").distinct()
      .union(openaire.select("Country","vatID").distinct()).distinct()
      .withColumnRenamed("Country","tmp")
      .groupBy("vatID")
      .agg(concat_ws("-",collect_set("tmp")).as("array") )
      .withColumn("Country", split(col("array"),"-").getItem(0) )
      .drop("array")

    val namesInfo = cordis.select("LegalName","vatID").distinct()
      .union(openaire.select("LegalName","vatID").distinct()).distinct()
      .withColumnRenamed("LegalName","tmp")
      .groupBy("vatID")
      .agg(concat_ws("-",collect_set("tmp")).as("array") )
      .withColumn("LegalName", split(col("array"),"-").getItem(0) )
      .drop("array")

//    merge.show()
//    countryInfo.show()
    println(merge.select("vatID").distinct().count())
    println(countryInfo.select("vatID").distinct().count() )

    val result = merge.join(countryInfo,Seq("vatID"),"left").join(namesInfo,Seq("vatID"),"left" )
      result.select("LegalName", "Country", "ProjectTitle").repartition(1).write.mode("overwrite").option("header","true")
          .csv(s"${path}result")

     result.show()
    println(result.select("vatID").distinct().count())


  }

  def main(args: Array[String]): Unit = {


    val dbpedia = spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("quote","\"").option("escape", "\"").csv(s"${path}dbpedia_northern_countries.csv")
      .withColumnRenamed("dblab","Country")

    val cordis = spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("quote","\"").option("escape", "\"")
      .option("multiline", "true").csv(s"${path}cordis_stem.csv")
      .withColumn("cleanName", trim(lower(col("LegalName"))))
      .select(cols.head, cols.tail:_*)

    val openaire = spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("quote","\"").option("escape", "\"")
      .option("multiline", "true").csv(s"${path}openaire_stem.csv")
      .withColumn("sDate", to_date(col("sDate")))
      .withColumn("eDate", to_date(col("eDate")))
      .filter(col("sDate") >=  "2010-01-01" )
      .filter(col("eDate") <= "2020-12-31" )
      .withColumn("cleanName", trim(lower(col("LegalName"))))
      .select(cols.head, cols.tail:_*)

    val cordis_northern = cordis.join(dbpedia.select("Country"), "Country")
    val openaire_northern = openaire.join(dbpedia.select(col("countryCode").as("Country")), "Country")

//    read(cordis, openaire, dbpedia);
//    stats(cordis_northern, openaire_northern)
    getResult(cordis_northern, openaire_northern)



    spark.close()
  }

}
