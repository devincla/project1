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
      println("Selecting total number of consumers on Branch 1..")
      //spark.sql("SELECT COUNT(*) FROM Branches WHERE branch = 'Branch1'").show()
      spark.sql("SELECT Branches.branch, sum(Conscounts.count) AS consumers FROM Conscounts INNER JOIN Branches ON Branches.bev = Conscounts.bev WHERE Branches.branch = 'Branch1' GROUP BY Branches.branch").show()
      println("Selecting total number of consumers on Branch 2...")
      spark.sql("SELECT Branches.branch, sum(Conscounts.count) AS consumers FROM Conscounts INNER JOIN Branches ON Branches.bev = Conscounts.bev WHERE Branches.branch = 'Branch2' GROUP BY Branches.branch").show()
      //spark.sql("SELECT COUNT(*) FROM Branches WHERE branch = 'Branch2'").show()
    }
    def scenario2() : Unit = {

      println("Selecting the most consumed beverage on Branch 1...")
      spark.sql("SELECT Branches.bev, sum(Conscounts.count) AS sales FROM Conscounts INNER JOIN Branches ON Branches.bev = Conscounts.bev WHERE Branches.branch = 'Branch1' GROUP BY Branches.bev ORDER BY sales DESC LIMIT 1").show()
      println("Selecting the least consumed beverage on Branch 2...")
      spark.sql("SELECT Branches.bev, sum(Conscounts.count) AS sales FROM Conscounts INNER JOIN Branches ON Branches.bev = Conscounts.bev WHERE Branches.branch = 'Branch2' GROUP BY Branches.bev ORDER BY sales ASC LIMIT 1").show()
      val df = spark.sql("SELECT Branches.bev, sum(Conscounts.count) AS sales FROM Conscounts INNER JOIN Branches ON Branches.bev = Conscounts.bev WHERE Branches.branch = 'Branch1' GROUP BY Branches.bev ORDER BY sales DESC")
      val medianDf = df.stat.approxQuantile("sales", Array(0.5), 0.25)
      println("The average beverage on Branch 2 is " + medianDf(0))

    }

    def scenario3() : Unit = {
      println("Selecting all beverages available in branches 1, 8, and 10...")
      spark.sql("SELECT DISTINCT bev FROM Branches WHERE branch IN ('Branch1', 'Branch8', 'Branch10') ORDER BY bev ASC").show(50)
      println("Selecting all beverages common to branches 4 and 7...")
      spark.sql("SELECT bev FROM Branches where branch = 'Branch4' INTERSECT SELECT bev from Branches where branch = 'Branch7'").show()
      //spark.sql("SELECT DISTINCT t1.bev\nFROM Branches t1\nINNER JOIN Branches t2\non t1.bev = t2.bev\nWHERE t1.branch = 'Branch3'\nAND t2.branch = 'Branch7' ORDER BY bev ASC").show(50)

    }

    def scenario4() : Unit = {
      println("\nDeleting table if exists...\n")
      spark.sql("DROP TABLE IF EXISTS partitionBranches")
      println("\nCreating a table with partitions on branches 1 & 8...\n")
      spark.sql("CREATE TABLE IF NOT EXISTS partitionBranches (bev STRING) PARTITIONED BY (branch STRING) row format delimited fields terminated by ','")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE partitionBranches PARTITION(branch='Branch8')")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE partitionBranches PARTITION(branch='Branch1')")
      spark.sql("DESCRIBE FORMATTED partitionBranches").show()
      spark.sql("SELECT * FROM partitionBranches").show()
    }

    def scenario5() : Unit = {
      println("\nDropping table if exists...\n")
      spark.sql("DROP TABLE IF EXISTS noteTable")
      println("\nCreating table...\n")
      spark.sql("CREATE TABLE IF NOT EXISTS noteTable (bev STRING, branch STRING) row format delimited fields terminated by ','")
      println("\nLoading data into table...\n")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE noteTable")
      println("\nAdding table note...\n")
      spark.sql("ALTER TABLE noteTable SET TBLPROPERTIES('notes' = 'This is a note')")
      spark.sql("SHOW TBLPROPERTIES noteTable").show()
      println("\nDeleting row from table...\n")
      spark.sql("ALTER TABLE partitionBranches DROP IF EXISTS PARTITION(branch='Branch1')")
      spark.sql("SELECT * FROM partitionBranches").show()


    }

    def scenario6() : Unit = {
      println("This is my future query")
      spark.sql("DROP TABLE IF EXISTS bevSales")
      println("\nCreating table...\n")
      spark.sql("CREATE TABLE IF NOT EXISTS bevSales (bev STRING, sales BIGINT, modifier STRING, bevType STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
      println("\nLoading data into table...\n")
      spark.sql("LOAD DATA LOCAL INPATH 'input/bevSalestxt.txt' INTO TABLE bevSales")
      println("\nOrganizing beverages by modifier popularity..\n")
      spark.sql("SELECT * FROM bevSales").show()
      println("\nOrganizing beverages by most to least popular beverage types...\n")
      spark.sql("select bevType, sum(sales) as totalSales from bevSales where bevType = 'cappuccino' GROUP BY bevType union " +
        "select bevType, sum(sales) as totalSales from bevSales where bevType = 'coffee' GROUP BY bevType union " +
        "select bevType, sum(sales) as totalSales from bevSales where bevType = 'espresso' GROUP BY bevType union " +
        "select bevType, sum(sales) as totalSales from bevSales where bevType = 'latte' GROUP BY bevType union " +
        "select bevType, sum(sales) as totalSales from bevSales where bevType = 'lite' GROUP BY bevType union " +
        "select bevType, sum(sales) as totalSales from bevSales where bevType = 'mocha' GROUP BY bevType union order by totalSales desc").show()

      println("\nOrganzing beverages by most to least popular modifier...\n")
      spark.sql("select modifier, sum(sales) as totalSales from bevSales where modifier = 'cold' GROUP BY modifier union " +
        "select modifier, sum(sales) as totalSales from bevSales where modifier = 'double' GROUP BY modifier union " +
        "select modifier, sum(sales) as totalSales from bevSales where modifier = 'icy' GROUP BY modifier union " +
        "select modifier, sum(sales) as totalSales from bevSales where modifier = 'large' GROUP BY modifier union " +
        "select modifier, sum(sales) as totalSales from bevSales where modifier = 'med' GROUP BY modifier union " +
        "select modifier, sum(sales) as totalSales from bevSales where modifier = 'mild' GROUP BY modifier union " +
        "select modifier, sum(sales) as totalSales from bevSales where modifier = 'small' GROUP BY modifier union " +
        "select modifier, sum(sales) as totalSales from bevSales where modifier = 'triple' GROUP BY modifier union " +
        "select modifier, sum(sales) as totalSales from bevSales where modifier = 'special' GROUP BY modifier order by totalSales desc").show()
    }

    def scenarioDrop() : Unit = {
      spark.sql("DROP TABLE Branches")
      spark.sql("DROP TABLE Conscounts")
      spark.sql("DROP TABLE newone1")
      spark.sql("DROP TABLE partitionbranches")
      spark.sql("DROP TABLE noteTable")
      spark.sql("DROP TABLE bevSales")
    }
//
//    spark.sql("CREATE TABLE IF NOT EXISTS Branches (bev STRING, branch STRING) row format delimited fields terminated by ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS Conscounts (bev STRING, count INT) row format delimited fields terminated by ','")
//    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE Branches")
//    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Conscount.txt' INTO TABLE Conscounts")
    var quit = false
    while(quit == false){

      println("\n\nChoose scenario: \n1) (Total consumers for Branch 1 & Branch 2)\n2) (Most, least, average)\n3) (All Bevs on Branches 1, 8, and 10 / Common Bevs between 7 & 4) \n4) (Partition) \n" +
        "5) (Note and Remove) \n6) (Future Query) \n7) (Drop Tables)\n8) (Quit Program)")
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
