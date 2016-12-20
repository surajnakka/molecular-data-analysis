import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import math.sqrt
import java.util.Calendar


object dpm {

         /* ------------- Calculate Dipole Moment ---------------
            Inputs : 1.Input file path
                     2.Output file path - DPM.txt
            Output : 1.Dipole Moment of all the atoms of each frame
                     2.Output text file with the same

            -The input text file contains x-coor.,y-coor.,z-coor. and charge of the atoms as the 5th,6th,7th and 9th attribute respectively
            -We push the data into an RDD - frame by frame
            -collect required data from RDD into an Array using ".collect" method
            -Compute the Dipole Moment = sqrt(x2+y2+z2)*charge 
            -Finally Sum up for each frame using reduce(_+_) method. 
         */
  def main(args: Array[String]) {
        val logFile = "/home/surajnakka/MSDataAnalysis/src/data/dataframe/frame" /* Should be the path of Input file on your system */
        val conf = new SparkConf().setAppName("MS Data") 
        val sc = new SparkContext(conf)            /* create a new spark context */
        var str = ""
        var dpmRDD : Double = 0.0 
        var tempRDD = Array.ofDim[Double](286850)
        var tempxyzRDD = Array.ofDim[Double](286850)
        var i = 0
        val start_time = Calendar.getInstance().getTime()
        str += "Starting Time %s\n".format(start_time)
        for (i <-0 to 100)                           /*For all the frames*/
        {  
                var logRDD = sc.textFile(logFile+"%s.txt".format(i),2).cache
                var tempx = logRDD.filter(line => line.contains("ATOM")).map(_.split("	")(4)).map(_.toFloat).map(x => x*x).collect //Extract x2
                var tempy = logRDD.filter(line => line.contains("ATOM")).map(_.split("	")(5)).map(_.toFloat).map(x => x*x).collect //Extract y2
		var tempz = logRDD.filter(line => line.contains("ATOM")).map(_.split("	")(6)).map(_.toFloat).map(x => x*x).collect //Extract z2
                var k = 0
                for(k <-0 to 286849) {
                     tempxyzRDD(k) = tempx(k)+tempy(k)+tempz(k)   //Compute x2+y2+z2 
                 }
                 var tempxyz = tempxyzRDD.map(x => sqrt(x))       //Compute sqrt(x2+y2+z2)
                var tempq = logRDD.filter(line => line.contains("ATOM")).map(_.split("	")(7)).map(_.toDouble).collect  //Extract Charge
                var j = 0
                for (j <-0 to 286849) {
                      tempRDD(j) = tempq(j)*tempxyz(j)            //Compute Charge*sqrt(x2+y2+z2)
                }                 
                dpmRDD = tempRDD.reduce(_+_)                      //Calculating Sum of Dipole Moment for each frame
        	println("Frame %s -> Dipole Moment : %f\n".format(i,dpmRDD))
       		str += "Frame %s -> Dipole Moment : %f\n".format(i,dpmRDD)     
	}
        val end_time = Calendar.getInstance().getTime()
        str += "Ending Time %s".format(end_time)
        printToFile(str)                                          //store the output to a text file               
  }
 
    /* - We have defined a function "prinToFile" to write the output to a text file 
       - It takes a string and a location as input
       - Writes the contents of the input string to the text file in the specified location
       */
  def printToFile(content: String, location: String =  "./output/DPM.txt") =
  Some(new java.io.PrintWriter(location)).foreach{f => try{f.write(content)}finally{f.close}} 
 
}
																																																																																																																
