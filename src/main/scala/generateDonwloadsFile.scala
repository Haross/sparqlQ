import java.io._

object generateDonwloadsFile {

  var entityType = "result"

  def main(args: Array[String]): Unit = {
    var baseURL = "https://zenodo.org/record/4587369/files/"
    var file = "part-$id$-"+entityType+".ttl116447"
    var size = 1098//1099;
    var digits = 5

    var lines:Seq[String] = Seq()

    for (i <- 0 to size  ) {

      var remainDigits = digits - i.toString.length
      var id = "0" * remainDigits + i
      var newFile = file.replace("$id$",id)
      println(newFile)
      lines = lines :+ "curl "+ baseURL+newFile+" -o " + newFile.replace("116447","")+"\n"
    }

    val file1 = new File(s"C:/Users/jflores/IdeaProjects/untitled/Output/${entityType}_URLs.bat")
    val bw = new BufferedWriter(new FileWriter(file1))
    for (l <- lines) {
      bw.write(l)
    }

    bw.close()

  }

}
