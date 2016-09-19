import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object UpdateAvailability {
 def main(args: Array[String]) {

   // setup the Spark Context named sc
   val conf = new SparkConf().setAppName("daily_availability")
   val sc = new SparkContext(conf)
   val sqlContext = new org.apache.spark.sql.SQLContext(sc)

   // folder on HDFS to pull the data from
   val folder_name = "hdfs://ec2-52-43-77-237.us-west-2.compute.amazonaws.com:9000/userTest/data.json"

   // read in the data from HDFS
   val file = sqlContext.read.json(folder_name)
	
   val schools = file.select('AVL).collect()
   val schoolsArr = schools.map(row => row.getSeq[org.apache.spark.sql.Row](0))
   schoolsArr.foreach(schools => {
     schools.map(row => print(row.getString("NAME"), row.getString("OCC"), row.getString("OPER")))
     print("\n")
   })   
   
   //file.show()
   //file.select("ges.open_spaces").show()
   //file.printSchema()
	

 }
}
