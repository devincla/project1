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
      val df = spark.sql("SELECT Branches.bev, sum(Conscounts.count) AS sales FROM Conscounts INNER JOIN Branches ON Branches.bev = Conscounts.bev WHERE Branches.branch = 'Branch1' GROUP BY Branches.bev ORDER BY sales DESC")
      val medianDf = df.stat.approxQuantile("sales", Array(0.5), 0.25)
      println("The average beverage on Branch 2 is " + medianDf(0))

    }

    def scenario3() : Unit = {

      spark.sql("SELECT DISTINCT bev FROM Branches WHERE branch IN ('Branch1', 'Branch8', 'Branch10') ORDER BY bev ASC").show()
      spark.sql("SELECT DISTINCT t1.bev\nFROM Branches t1\nINNER JOIN Branches t2\non t1.bev = t2.bev\nWHERE t1.branch = 'Branch3'\nAND t2.branch = 'Branch7' ORDER BY bev ASC").show(50)

    }

    def scenario4() : Unit = {
      println("\nCreating a table with partitions on branches 1 & 8...\n")
      //spark.sql("CREATE TABLE IF NOT EXISTS partitionBranches (bev STRING) PARTITIONED BY (branch STRING) row format delimited fields terminated by ','")
      //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE partitionBranches PARTITION(branch='Branch8')")
      //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE partitionBranches PARTITION(branch='Branch1')")
      spark.sql("DESCRIBE FORMATTED partitionBranches").show()
      spark.sql("SELECT * FROM partitionBranches").show()
    }

    def scenario5() : Unit = {
      //spark.sql("DROP TABLE noteTable")
      println("\nCreating table...\n")
      spark.sql("CREATE TABLE IF NOT EXISTS noteTable (bev STRING, branch STRING) row format delimited fields terminated by ','")
      //println("\nLoading data into table...\n")
      //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE noteTable")
      println("\nAdding table note...\n")
      spark.sql("ALTER TABLE noteTable SET TBLPROPERTIES('notes' = 'This is a note')")
      spark.sql("SHOW TBLPROPERTIES noteTable").show()
//      println("\nDeleting row from table...\n")
//      spark.sql("ALTER TABLE partitionbranches DROP IF EXISTS PARTITION(branch='Branch1')")


    }

    def scenario6() : Unit = {
      println("This is my future query")
      //spark.sql("DROP TABLE bevSales")
//      println("\nCreating table...\n")
      spark.sql("CREATE TABLE IF NOT EXISTS bevSales (bev STRING, sales BIGINT, modifier STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//      println("\nLoading data into table...\n")
      //spark.sql("LOAD DATA LOCAL INPATH 'input/bevSalestxt.txt' INTO TABLE bevSales")
      println("\nOrganizing beverages by modifier popularity..\n")
      spark.sql("select sum(sales) as totalSales from bevSales where bev like '%cold%' union " +
        "select sum(sales) as totalSales from bevSales where bev like '%double%' union " +
        "select sum(sales) as totalSales from bevSales where bev like '%icy%' union " +
        "select sum(sales) as totalSales from bevSales where bev like '%large%' union " +
        "select sum(sales) as totalSales from bevSales where bev like '%med%' union " +
        "select sum(sales) as totalSales from bevSales where bev like '%mild%' union " +
        "select sum(sales) as totalSales from bevSales where bev like '%small%' union " +
        "select sum(sales) as totalSales from bevSales where bev like '%triple%' union " +
        "select sum(sales) as totalSales from bevSales where bev like '%special%' order by totalSales desc").show()

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

      println("\n\nChoose scenario: \n1 (Select)\n2 (Most, least, average)\n3 (Common Bevs) \n4 (Partition) \n5 (Note and Remove) \n6 (Future Query) \n7 (Drop Tables)\n8 (Quit Program)")
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
