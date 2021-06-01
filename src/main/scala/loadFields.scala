
import org.apache.jena.query.QueryFactory
import org.apache.spark.sql.SparkSession
import virtuoso.jena.driver._
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory
import org.apache.spark.sql.Row
import virtuoso.jena.driver.VirtuosoUpdateFactory
import org.apache.jena.sparql.engine.http.QueryEngineHTTP
import org.apache.spark.sql.functions.col
object loadFields {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.driver.memory", "5g")
    .config("spark.driver.maxResultSize", "4g")
    //    .config("spark.executor.memory","9g")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val connection = new VirtGraph("jdbc:virtuoso://localhost:1111", "dba", "dba")
  val baseURL = "http://data.europa.eu/s66#"
  val graphURL = "http://data.europa.eu/s66/Projects#"

  val pathFile =s"C:/Users/jflores/Downloads/Virtuoso/Virtuoso/06_Data/EuroSciVoc/classification_all.csv"

  def insertFields(): Unit ={

    val df = spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("quote","\"").option("escape", "\"").csv(s"$pathFile")

//    df.filter(col("RCN") === 104808).show
    df.printSchema()
//    df.groupBy("Project Id").count.orderBy(desc("count")).show
    var pIn = 0
    var pOut = 0
    df.select("Project Id","RCN", "Category label").distinct.collect()
      .foreach{
        case Row( projectId: String, rcn: Integer, category:String) =>
          val projectURI = isRCNInGraph(rcn.toString)
          if (projectURI != "" ){
            println(s" $rcn $category $projectURI")
            insert(graphURL, projectURI, category)
            pIn = pIn + 1
          } else {
            pOut = pOut+1
          }

      }

    println(s"RCN $pIn")
    println(s"RCN OUT $pOut")

  }

  def printSciences() = {
    val df = spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("quote","\"").option("escape", "\"").csv(s"$pathFile")

    val data = df.select( "Category label").distinct
    data.show(974,false)

  }

  def insert(graph: String, projectURI: String, category: String) = {

    val scienceURI = s"${baseURL}hasFieldOfScience"
    val str = "PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>" +
      "INSERT INTO GRAPH <"+graph+"> { <"+projectURI+"> <"+scienceURI+"> \""+category+"\"^^xsd:string}"
    println(str)
    val vur = VirtuosoUpdateFactory.create(str, connection)
    vur.exec
  }

  def deleteGraph(graphURI: String) = {

    val str= s"CLEAR GRAPH <${graphURI}>"
    println(str)
    val vurt = VirtuosoUpdateFactory.create(str, connection)
    vurt.exec()
  }

  def deleteTriple(graphURI: String, s: String, p: String, o: String) = {

    val q = s"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>" +
      s" DELETE FROM GRAPH <${graphURI}> { <${s}> <${p}>"+
      "\""+o+"\"^^xsd:string}"
    println(q)
    val vur = VirtuosoUpdateFactory.create(q, connection)
    vur.exec()
  }

  def deleteFields(graphURI: String) = {
    val scienceURI = s"${baseURL}hasFieldOfScience"
//    val q = s"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>" +
//      s" DELETE {?s <${scienceURI}> ?p } where { ?s <${scienceURI}> ?p}"
//    println(q)
//    val vur = VirtuosoUpdateFactory.create(q, connection)
//    vur.exec()
    val qAll = s"select * from <${graphURI}> where {?s <${scienceURI}> ?science}"
    println(qAll)
    val vqe = VirtuosoQueryExecutionFactory.create(qAll, connection)
    val results = vqe.execSelect
    while ( {results.hasNext}) {
      val rs = results.nextSolution
      val s = rs.get("s")
      val science = rs.get("science")
//
      val q = s"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>" +
        s" DELETE FROM GRAPH <${graphURI}> { <${s}> <${scienceURI}>"+ " \""+science+"\""
      println(q)
      val vur = VirtuosoUpdateFactory.create(q, connection)
      vur.exec()
//      try {
//        val exec = new QueryEngineHTTP("http://localhost:8890/sparql/", q)
//        try {
//          exec.execSelect
//        } finally if (exec != null) exec.close()
//      }
    }


  }


  def isRCNInGraph(rcn: String): String = {
//    val q = "SELECT * FROM <http://data.europa.eu/s66#> WHERE {?s a <http://data.europa.eu/CORDIS/ontology#Project>." +
//       "?s <http://data.europa.eu/CORDIS/ontology#hasRCN> \"" + rcn +"\"}"

    val q = "PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>" +
      s"select * from <${graphURL}> where {?project a <http://data.europa.eu/s66#Project>." +
      s"?project <http://data.europa.eu/s66#hasIdentifier> ?pId. " +
      "?pId <http://data.europa.eu/s66#name> \"rcn\"^^xsd:string. " +
      "?pId <http://data.europa.eu/s66#value> \"" + rcn+ "\"^^xsd:string}"
//    "?pId <http://data.europa.eu/s66#name> ?rcn. filter(?rcn = \"rcn\"^^xsd:string). }"

    try {
      val exec = new QueryEngineHTTP("http://localhost:8890/sparql/", q)
      try {
        val results = exec.execSelect
//        println(exec.getQuery)
//      ResultSetFormatter.out(results)
        if(results.hasNext){
          val rs = results.nextSolution
        //println("*"+rs.get("nom").toString+"*")
          return rs.get("project").toString
        }
      } finally if (exec != null) exec.close()
    }
    ""
  }

  def virtuosoIn() = {

    val sparql = QueryFactory.create("SELECT distinct ?graph WHERE { GRAPH ?graph { ?s ?p ?o } } limit 100")
    val vqe = VirtuosoQueryExecutionFactory.create(sparql, connection)

    val results = vqe.execSelect
    println("before while")
    println(results.hasNext)
    while ( {results.hasNext }) {
      val result = results.nextSolution
      val graph = result.get("graph")
   /*   val s = result.get("s")
      val p = result.get("p")
      val o = result.get("o")*/
      println(s"*${graph}*")
//      println(graph + " { " + s + " " + p + " " + o + " . }")
    }
  }

  def main(args: Array[String]): Unit = {
    println("hola")


    val option = 4
    option match {
      case 1 =>
        println("Option to test rcn is in graph")
        val url = isRCNInGraph("205854")
        println(s"$url")
      case 2 =>
        println("Option to test inserting")
        insert(graphURL, "http://data.europa.eu/s66/Projects#1dd99ba8-3719-38bd-8986-865ac3931cfb","javi")
      case 3 =>
        println("Option to delete triple")
        deleteTriple(graphURL, "http://data.europa.eu/s66/Projects#1dd99ba8-3719-38bd-8986-865ac3931cfb", "http://data.europa.eu/CORDIS/ontology#hasFieldOfScience", "javi")
      case 4 =>
        println("Option to read csv and generate fields")
        insertFields()
      case 5 =>
        println("NOT WORKING Option to delete inserted fields triples")
        deleteFields(graphURL)
      case 6 =>
        printSciences()
      case _ =>
        println("no option selected")
    }

//    if (url != ""){
//      println("existe")
//    } else {
//      println("no existe")
//    }
//    deleteGraph("http://patent/ja#")
//
//    println()
//    readCSV()
//    println(virtuosoIn())
//    readCSV()
//    val rcn = "123"
//val q = "PREFIX co: <http://data.europa.eu/CORDIS/ontology#> " +
//  "SELECT ?s WHERE {  GRAPH <http://data.europa.eu/s66#> {?s a co:Project." +
//  s"?s <http://data.europa.eu/CORDIS/ontology#hasRCN> $rcn}}"
//println(q)

  }


}
