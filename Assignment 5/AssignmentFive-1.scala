package assignment.five

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import scala.reflect.io.Directory
import java.io.File

object AssignmentFive extends App {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  var sparkSession: SparkSession = SparkSession.builder().
    config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.serializer", classOf[KryoSerializer].getName).
    config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName).
    master("local[*]")
    .appName("lastassignment").getOrCreate()

  SedonaSQLRegistrator.registerAll(sparkSession)
  SedonaVizRegistrator.registerAll(sparkSession)

  val resourseFolder = System.getProperty("user.dir") + "/src/test/resources/"
  val csvPolygonInputLocation = resourseFolder + "testenvelope.csv"
  val csvPointInputLocation = resourseFolder + "testpoint.csv"
  val firstpointdata = resourseFolder + "outputdata/firstpointdata"
  val firstpolydata = resourseFolder + "outputdata/firstpolygondata"

  println("Q2.1")
  firstPointQuery()
  println("Q2.2")
  secondPointQuery()
  println("Q2.3")
  firstPloygonQuery()
  println("Q2.4")
  secondPolygonQuery()
  println("Q2.5")
  JoinQuery()

  println("Assignment Five Done!!!")

  def firstPointQuery(): Unit = {
    //Read the given testpoint.csv file in csv format and write in delta format and save named firstpointdata
    val df = sparkSession.read.format("csv").load(csvPointInputLocation) //working
    val directory = new Directory(new File(resourseFolder + "outputdata/firstpointdata"))
    directory.deleteRecursively()
    df.write.format("delta").mode("overwrite").save(firstpointdata) //working
  }

  def secondPointQuery(): Unit = {
    //Read the firstpointdata in delta format. Print the total count of the points.
    val df = sparkSession.read.format("delta").load(firstpointdata) // working
    println("Total count of points: " + df.count()) //working
  }

  def firstPloygonQuery(): Unit = {
    //Read the given testenvelope.csv in csv format and write in delta format and save it named firstpolydata
    val df = sparkSession.read.format("csv").load(csvPolygonInputLocation) //working
    val directory = new Directory(new File(resourseFolder + "outputdata/firstpolygondata"))
    directory.deleteRecursively()
    df.write.format("delta").mode("overwrite").save(firstpolydata) //working
  }

  def secondPolygonQuery(): Unit = {
    //Read the firstpolydata in delta format. Print the total count of the polygon
    val df = sparkSession.read.format("delta").load(firstpolydata) // working
    println("Total count of polygon: " + df.count()) //working
  }

  def JoinQuery(): Unit = {
    //Read the firstpointdata in delta format and find the total count for point pairs where distance between the points within a pair is less than 2.
    var read_points = sparkSession.read.parquet(firstpointdata) //working
    read_points = read_points.toDF() //working
    read_points.createOrReplaceTempView("pointsdata") //working

    read_points = sparkSession.sql("SELECT ST_Point(CAST(pointsdata._c0 as Decimal(24,20)), CAST(pointsdata._c1 AS Decimal(24,20)))  AS point FROM pointsdata") //working
    read_points.createOrReplaceTempView("pointsDF") //working

    var joinPoints = sparkSession.sql("SELECT COUNT(*) FROM pointsDF p1 JOIN pointsDF p2 WHERE p1.point != p2.point AND ST_Distance(p1.point, p2.point) < 2") //working
    joinPoints.show() //working
  }

}