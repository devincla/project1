import org.apache.spark.sql.SparkSession
import java.util.Scanner
import org.apache.spark.sql._
object project0 {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession.builder()
      .appName("HiveTest5")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("created spark session")
    val scanner = new Scanner(System.in)
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")


    def scenario1() : Unit = {
      spark.sql("SELECT COUNT(*) FROM Branches WHERE branch = 'Branch1'").show()
      spark.sql("SELECT COUNT(*) FROM Branches WHERE branch = 'Branch2'").show()
    }
    def scenario2() : Unit = {

      spark.sql("SELECT Branches.bev, sum(Conscounts.count) AS sales FROM Conscounts INNER JOIN Branches ON Branches.bev = Conscounts.bev WHERE Branches.branch = 'Branch1' GROUP BY Branches.bev ORDER BY sales DESC LIMIT 1").show()
      spark.sql("SELECT Branches.bev, sum(Conscounts.count) AS sales FROM Conscounts INNER JOIN Branches ON Branches.bev = Conscounts.bev WHERE Branches.branch = 'Branch2' GROUP BY Branches.bev ORDER BY sales ASC LIMIT 1").show()
      spark.sql("SELECT * FROM Branches ORDER BY bev LIMIT 1").show()

    }

    def scenario3() : Unit = {

      spark.sql("SELECT DISTINCT bev FROM Branches WHERE branch IN ('Branch1', 'Branch8', 'Branch10') ORDER BY bev ASC").show()
      spark.sql("SELECT DISTINCT t1.bev\nFROM Branches t1\nINNER JOIN Branches t2\non t1.bev = t2.bev\nWHERE t1.branch = 'Branch3'\nAND t2.branch = 'Branch7' ORDER BY bev ASC").show(50)

    }

    def scenario4() : Unit = {
      spark.sql("CREATE TABLE IF NOT EXISTS BranchesCopy (bev STRING) PARTITIONED BY row format delimited fields terminated by ','")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE BranchesCopy")

    }

    def scenario5() : Unit = {

    }

    def scenario6() : Unit = {
      println("This is my future query")

    }

    def scenarioDrop() : Unit = {
      spark.sql("DROP TABLE Branches")
      spark.sql("DROP TABLE Conscounts")
    }
//
//    spark.sql("CREATE TABLE IF NOT EXISTS Branches (bev STRING, branch STRING) row format delimited fields terminated by ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS Conscounts (bev STRING, count INT) row format delimited fields terminated by ','")
//    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE Branches")
//    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Conscount.txt' INTO TABLE Conscounts")
    var quit = false
    while(quit == false){

      println("Choose scenario 1, 2, 3, 4, 5, 6, 7, or 8")
      val i = scanner.nextInt()
      i match {
        case 1 => scenario1()
        case 2 => scenario2()
        case 3 => scenario3()
        case 4 => scenario4()
        case 5 => scenario5()
        case 6 => scenario6()
        case 7 => scenarioDrop()
        case 8 => quit = true


        case default => println("Invalid option, please try again.")
      }

    }

  }


}
