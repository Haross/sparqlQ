import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_set, concat_ws, desc, lit, lower, size, split, trim, udf, upper, when}

object US_RO_01 {

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
  val cols = Seq("orgName","cleanName", "orgURI", "title")

  val cordis = spark.read
    .option("header", "true").option("inferSchema", "true")
    .option("quote","\"").option("escape", "\"").csv(s"${path}cordis_withTitles.csv")
    .withColumn("cleanName", trim(lower(col("orgName"))))
    .select(cols.head, cols.tail:_*)

  val openaire = spark.read
    .option("header", "true").option("inferSchema", "true")
    .option("quote","\"").option("escape", "\"").csv(s"${path}openaire_withTitles2.csv")
    .withColumn("cleanName", trim(lower(col("orgName"))))
    .select(cols.head, cols.tail:_*)

  val scholarly = spark.read
    .option("header", "true").option("inferSchema", "true")
    .option("quote","\"").option("escape", "\"").csv(s"${path}scholarly_withTitles.csv")
    .withColumn("cleanName", trim(lower(col("orgName"))))
    .select(cols.head, cols.tail:_*)
  var cleanNames = Seq[String]()
  var cordisNames = Seq[String]()
  var scholarlyNames = Seq[String]()
  var openaireNames = Seq[String]()

  var cordisNames_dup = Seq[String]()
  var scholarlyNames_dup = Seq[String]()
  var openaireNames_dup = Seq[String]()

  var mapDS = Map[String, String]()

  val mergeUDF = udf(mergeURI(_:String, _:String): String)
  val checkIsIn = udf(duplicate( _:String, _:String): String)

  def stats () = {
    println(s"CORDIS organizations ${cordis.select("orgName").distinct.count()}")
    println(s"Scholarly organizations ${scholarly.select("orgName").distinct.count()}")
    println(s"Scholarly organizations uri ${scholarly.select("orgURI").distinct.count()}")
    println(s"Openaire organizations ${openaire.select("orgURI").distinct.count()}")

    println(s"CORDIS results ${cordis.select("title").distinct.count()}")
    println(s"Scholarly results ${scholarly.select("title").distinct.count()}")
    println(s"Openaire results ${openaire.select("title").distinct.count()}")

    println(s"CORDIS results ${cordis.select("title").count()}")
    println(s"Scholarly results ${scholarly.select("title").count()}")
    println(s"Openaire results ${openaire.select("title").count()}")

    val cordisScholarly = cordis.select("cleanName").distinct.union(scholarly.select("cleanName").distinct)
//      .union(openaire.select("cleanName").distinct)

    cleanNames = Seq[String]()
    cordisScholarly.groupBy("cleanName").count.collectAsList().forEach( row => {

      if(row.getAs[Long]("count") > 1) // then duplicated
        cleanNames = cleanNames :+ row.getAs[String]("cleanName")
    })
    println(s"overlap cordis-scholarly is: ${cleanNames.size}")
    println(cleanNames)

    val cordisOpenaire = cordis.select("cleanName").distinct.union(openaire.select("cleanName").distinct)
    cleanNames = Seq[String]()
    cordisOpenaire.groupBy("cleanName").count.collectAsList().forEach( row => {

      if(row.getAs[Long]("count") > 1) // then duplicated
        cleanNames = cleanNames :+ row.getAs[String]("cleanName")
    })
    println(s"overlap cordis-openaire is: ${cleanNames.size}")
    var totalTO = openaire.filter(col("cleanName").isin(cleanNames.toSeq:_*)).select("title").distinct().count()
    var totalTC = cordis.filter(col("cleanName").isin(cleanNames.toSeq:_*)).select("title").distinct().count()
    println(s"Cordis titles ${totalTC}")
    println(s"new titles ${totalTC-totalTO}")
    println(cleanNames)

    val openaireScholarly = openaire.select("cleanName").distinct.union(scholarly.select("cleanName").distinct)
    cleanNames = Seq[String]()
    openaireScholarly.groupBy("cleanName").count.collectAsList().forEach( row => {

      if(row.getAs[Long]("count") > 1) // then duplicated
        cleanNames = cleanNames :+ row.getAs[String]("cleanName")
    })
    println(s"overlap openaire-scholarly is: ${cleanNames.size}")
    println(cleanNames)
    totalTO = openaire.filter(col("cleanName").isin(cleanNames.toSeq:_*)).select("title").distinct().count()
    totalTC = scholarly.filter(col("cleanName").isin(cleanNames.toSeq:_*)).select("title").distinct().count()
    println(s"scholarly titles ${totalTC}")
    println(s"new titles ${totalTC-totalTO}")


  }


  def read(): Unit ={





//    all.select("cleanName").groupBy("cleanName").count().show

    cordis.select("cleanName").groupBy("cleanName").count.collectAsList().forEach( row => {
        cordisNames = cordisNames :+ row.getAs[String]("cleanName")
      if (row.getAs[Long]("count") > 1) {
        cordisNames_dup = cordisNames_dup :+ row.getAs[String]("cleanName")
      }
    })
    scholarly.select("cleanName").groupBy("cleanName").count.collectAsList().forEach( row => {
      scholarlyNames = scholarlyNames :+ row.getAs[String]("cleanName")
      if (row.getAs[Long]("count") > 1) {
        scholarlyNames_dup = scholarlyNames_dup :+ row.getAs[String]("cleanName")
      }
    })
    openaire.select("cleanName").groupBy("cleanName").count.collectAsList().forEach( row => {
      openaireNames = openaireNames :+ row.getAs[String]("cleanName")
      if (row.getAs[Long]("count") > 1) {
        openaireNames_dup = openaireNames_dup :+ row.getAs[String]("cleanName")
      }
    })

    val all = cordis.withColumn("duplicate", col("cleanName").isin(cordisNames_dup:_*))
      .union(scholarly.withColumn("duplicate", col("cleanName").isin(scholarlyNames_dup:_*)))
      .union(openaire.withColumn("duplicate", col("cleanName").isin(openaireNames_dup:_*)))
//    println(s"total ${all.count}")
//    println(s"total ${all.select("orgURI").distinct().count}")
//    all.filter(upper(col("orgName")) === "AGENCIA ESTATAL CONSEJO SUPERIOR DEINVESTIGACIONES CIENTIFICAS").show(false)
//    all.filter(upper(col("orgName")) === "AGENCIA ESTATAL CONSEJO SUPERIOR DE INVESTIGACIONES CIENTIFICAS").show(false)



//    val df = all.withColumn( "inOther",
//      checkIsIn(col("cleanName"), col("orgURI")))
//
//    df.filter(col("duplicate") === true).show(100)


    val tmp2 = all.select("orgURI","cleanName").groupBy("cleanName")
      .agg(concat_ws(",",collect_set("orgURI")).as("orgs"))
      .withColumn("joins", size(split(col("orgs"), ",")))


    tmp2.filter(upper(col("cleanName")).contains("AGENCIA ESTATAL CONSEJO SUPERIOR DEINVESTIGACIONES CIENTIFICAS")).show(false)
    tmp2.filter(upper(col("cleanName")).contains("AGENCIA ESTATAL CONSEJO SUPERIOR DE INVESTIGACIONES CIENTIFICAS")).show(false)


    mapDS = Map[String, String]()
    tmp2.filter(col("joins") > 1).select("cleanName", "orgs").collect.foreach {
      case Row(cleanName: String, orgs: String) =>
        mapDS = mapDS + (cleanName -> orgs)
    }

    val pre = all.withColumn("newOrgURI", mergeUDF(col("orgURI"), col("cleanName")))

    val just1 = pre.select("newOrgURI","orgName").groupBy("newOrgURI")
      .agg(concat_ws(",",collect_set("orgName")).as("names"))

    val df2 = pre.select("newOrgURI", "title").join(just1, Seq("newOrgURI"))
      .distinct().withColumn("numberDuplicatesOrg", size(split(col("names"), ",")))

    df2.show()
    df2.filter(upper(col("names")).contains("AGENCIA ESTATAL CONSEJO SUPERIOR DEINVESTIGACIONES CIENTIFICAS")).show(false)
    df2.filter(upper(col("names")).contains("AGENCIA ESTATAL CONSEJO SUPERIOR DE INVESTIGACIONES CIENTIFICAS")).show(false)
    //    df2.filter(upper(col("orgName")) === "AGENCIA ESTATAL CONSEJO SUPERIOR DE INVESTIGACIONES CIENTIFICAS").show(false)

    println("jla")
    df2.select(split(col("names"), ",").getItem(0).as("orgName"),col("title"))
      .groupBy("orgName").count().as("numberOfPublications").orderBy(desc("numberOfPublications.count"))
      .repartition(1).write.mode("overwrite").option("header","true")
      .csv(s"${path}US_RO_01_new.csv")

    //
    //    all.select("cleanName")
//    df
//      .filter(col("orgURI") === "https://w3id.org/scholarlydata/organisation/marum-university-of-bremen")
//      .show(30,false)
//    df.show(400)
//    cordis.show()
//    openaire.show()
//    scholarly.show()
//    cordis.show
//
//    val overlap = openaire.join(scholarly, Seq("cleanName")).show
//    openaire.
    //hits ggmbh

  }

  def mergeURI(uri: String, cleanName: String): String = {

    val default = (cleanName, uri)
    mapDS.find(_._2.contains(uri)).getOrElse(default)._2.split(",")(0)
    /*if(mapDS.contains(cleanName))
      return mapDS.get(cleanName).get.split(",")(0)
    uri*/
  }

  def duplicate( name:String, uri:String): String = {


    if (cleanNames.contains(name)) {

      var repositories = Seq[String]()

      if(cordisNames.contains(name) & !uri.contains("data.europa.eu"))
          repositories = repositories :+ "cordis"

      if(scholarlyNames.contains(name) & !uri.contains("scholarly"))
          repositories = repositories :+ "scholarly"

      if(openaireNames.contains(name) & !uri.contains("openaire"))
          repositories = repositories :+ "openaire"

//      println(s"returning ${repositories.toString()}")
      return repositories.toString()

    }
    "";

  }


  def main(args: Array[String]): Unit = {
//      read();
    stats()
//    US_RO_01 us = new US_RO_01()
    spark.close()
  }

}
