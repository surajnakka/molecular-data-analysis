import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import java.util.Calendar


object som {

         /* ------------- Calculate Sum of Masses ---------------
            Inputs : 1.Input file path
                     2.Output file path - SOM.txt
            Output : 1.Sum of masses of all the atoms
                     2.Output text file with the information of sum of masses

            -The input text file contains Mass of the atom as the 8th attribute  
            -We push the data into an RDD
            -collect required data from RDD into an Array using ".collect" method
            -Compute the sum of masses using reduce(_+_) method. 
         */
  def main(args: Array[String]) {
        val logFile = "./src/data/data.txt" /* Should be the path of some file on your system - passed as argument 1 */
        val conf = new SparkConf().setAppName("MS Data") 
        val sc = new SparkContext(conf)            /* create a new spark context */
        var str = ""
        var somRDD : Float = 0 
        val start_time = Calendar.getInstance().getTime()
        str += "Starting Time %s\n".format(start_time)
        val massRDD = sc.textFile(logFile,2).filter(line => line.contains("ATOM")).map(_.split("	")(8)).map(_.toFloat).collect
                         /*filtering lines containing "ATOM" and getting only Mass column*/  /*Collecting data into an Array[Float] */
                                                
        var i = 0
        for (i <-0 to 100)                           /*For all the frames*/
        {  
                var tempRDD = massRDD.slice(286850*i,286850*(i+1))     /*slicing each frame into temporary array*/            
                somRDD = tempRDD.reduce(_+_)                            /*Calculating sum of masses for each frame*/
        	println("Frame %s -> Total Mass : %f\n".format(i,somRDD))
       		str += "Frame %s -> Total Mass : %f\n".format(i,somRDD)     
	}
        val end_time = Calendar.getInstance().getTime()
        str += "Ending Time %s".format(end_time)
        printToFile(str)                          /*store the output to a text file */                 
  }
 
    /* - We have defined a function "prinToFile" to write the output to a text file 
       - It takes a string and a location as input
       - Writes the contents of the input string to the text file in the specified location
       */
  def printToFile(content: String, location: String =  "./output/SOM.txt") =
  Some(new java.io.PrintWriter(location)).foreach{f => try{f.write(content)}finally{f.close}} 
 
}
																																																																																																																
