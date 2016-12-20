import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import math.sqrt
import java.util.Calendar


object moiz {

         /* ------------- Calculate Moment of Inertia over z-Axis ---------------
            Inputs : 1.Input file path
                     2.Output file path - MOIZ.txt
            Output : 1.Moment of Inertia over z-Axis of all the atoms
                     2.Output text file with the information of Moment of Inertia over z-Axis for each frame

            -The input text file contains Mass of the atom as the 8th attribute 
            -The input text file contains x-coord,y-coord of the atom as the 4th,5th attributes respectively 
            -We push the data into an RDD
            -collect required data from RDD into an Array using ".collect" method
            -Compute Moment of Inertia over z-axis as mass*sqrt(x2+y2)
            -Compute the sum of Moment of Inertia over z-axis using reduce(_+_) method. 
         */
  def main(args: Array[String]) {
        val logFile = "./src/data/dataframe/frame" /* Should be the path of some file on your system */
        val conf = new SparkConf().setAppName("MS Data") 
        val sc = new SparkContext(conf)            /* create a new spark context */
        var str = ""
        var moizRDD : Double = 0.0 
        var tempRDD = Array.ofDim[Double](286850)            
        var tempxyRDD = Array.ofDim[Double](286850)
        val start_time = Calendar.getInstance().getTime()
        str += "Starting Time %s\n".format(start_time)
        var i = 0
        for (i <-0 to 100)                           /*For all the frames*/
        {  
                var logRDD = sc.textFile(logFile+"%s.txt".format(i),2).cache
                var tempx = logRDD.filter(line => line.contains("ATOM")).map(_.split("	")(4)).map(_.toFloat).map(x => x*x).collect //get x2
                var tempy = logRDD.filter(line => line.contains("ATOM")).map(_.split("	")(5)).map(_.toFloat).map(x => x*x).collect //get y2
                var k = 0
                for(k <-0 to 286849) {              //286850 is the frame size of the dataset this code is written for. - also we can use tempx.length-1 or tempy.length-1, but it wastes a cycle of computation
                     tempxyRDD(k) = tempx(k)+tempy(k)               //compute x2+y2
                 }
                 var tempxy = tempxyRDD.map(x => sqrt(x))           //compute sqrt(x2+y2)
                var tempm = logRDD.filter(line => line.contains("ATOM")).map(_.split("	")(8)).map(_.toDouble).collect  //get Mass column
                var j = 0
                for (j <-0 to 286849) {
                      tempRDD(j) = tempm(j)*tempxy(j)               //compute Mass*sqrt(x2+y2)
                }                 
                moizRDD = tempRDD.reduce(_+_)                       /*Calculating sum of Moment of Inertia over z-axis for each frame*/
        	println("Frame %s -> Moment of Inertia over z-Axis : %f\n".format(i,moizRDD))
       		str += "Frame %s -> Moment of Inertia over z-Axis : %f\n".format(i,moizRDD)     
	}
        val end_time = Calendar.getInstance().getTime()
        str += "Ending Time %s".format(end_time)
        printToFile(str)                          /*store the output to a text file */                 
  }
 
    /* - We have defined a function "prinToFile" to write the output to a text file 
       - It takes a string and a location as input
       - Writes the contents of the input string to the text file in the specified location
       */
  def printToFile(content: String, location: String =  "/home/surajnakka/MSDataAnalysis/output/MOIZ.txt") =
  Some(new java.io.PrintWriter(location)).foreach{f => try{f.write(content)}finally{f.close}} 
 
}
																																																																																																																
