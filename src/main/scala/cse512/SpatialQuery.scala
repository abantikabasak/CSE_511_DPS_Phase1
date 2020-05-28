package cse512


import org.apache.spark.sql.SparkSession
import scala.math.sqrt
import scala.math.pow


object SpatialQuery extends App{


  
  def ST_Contains(queryRect:String, pointStr:String): Boolean = {
    
    // Null checks
    if(pointStr==null || pointStr.isEmpty() || queryRect==null || queryRect.isEmpty() )
      return(false)
      
    // Coordinates of the point
    val point = pointStr.split(",").map(_.toDouble) 
    // Coordinates of the query rectangle (x1,y1), (x2,y2)
    val rectangle = queryRect.split(",").map(_.toDouble)
    var min_x = if (rectangle(0) < rectangle(2)) rectangle(0) else rectangle(2) 
    var max_x = if (rectangle(0) > rectangle(2)) rectangle(0) else rectangle(2) 
    var min_y = if (rectangle(1) < rectangle(3)) rectangle(1) else rectangle(3) 
    var max_y = if (rectangle(1) > rectangle(3)) rectangle(1) else rectangle(3) 
    
    if(point(0) >= min_x && point(0) <= max_x && point(1) >= min_y && point(1) <= max_y) {
      return true
    } else {
      return false
    }
  
  }

  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean = {
    
    /*
    //Check whether the first point is empty or null, then return false
    if(pointString1 == null || pointString1.isEmpty())
      return(false)
    //Check whether the second point is empty or null, then return false
    if(pointString2 == null || pointString2.isEmpty())
      return(false)
    //Distance has to be positive
    if(distance <= 0)
      return(false)
    
    //Extract x,y co-ordinates of point1
    val point1_coords = pointString1.split(",")
    var point_x1 = point1_coords(0).toDouble
    var point_y1 = point1_coords(1).toDouble

    //Extract x,y co-ordinates of point2
    val point2_coords = pointString2.split(",")
    var point_x2 = point2_coords(0).toDouble
    var point_y2 = point2_coords(1).toDouble
    
   

    var dist = sqrt(pow((point_x2 - point_x1),2) + pow((point_y2 - point_y1),2))


    //Check if the calculated distance is greater than the required distance or not
    if(dist > distance)
    {
      return(false)
    }
    else
    {
      return(true)
    }

    */
    // Null checks
    if(pointString1 == null || pointString1.isEmpty() || pointString2 == null || pointString2.isEmpty() || distance <= 0)
      return(false)
    
    val Array(pointX1,pointY1) = pointString1.split(",").map(_.toDouble)
    val Array(pointX2,pointY2) = pointString2.split(",").map(_.toDouble)

    val distPoints = Math.sqrt(((pointX2 - pointX1) * (pointX2 - pointX1)) + ((pointY2 - pointY1) * (pointY2 - pointY1)))
    if (distPoints > distance){
      return false
    } else {
      return true
    }



  }


  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()
    //println(resultDf.count())
    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()
    //println(resultDf.count())


    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()
    //println(resultDf.count())

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")

    resultDf.show()
    //println(resultDf.count())
    return resultDf.count()
  }
}
