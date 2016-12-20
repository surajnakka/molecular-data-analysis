import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import java.util.Calendar


object com {

         /* ------------- Calculate Center of Mass ---------------
            Inputs : 1.Input file path  - Two in this module --This module needs MOI as well as SOM to be pre-computed
                     2.Output file path - COM.txt
            Output : 1.Center of Mass of all the atoms for each frame
                     2.Output text file with the information of Center of Mass for each frame

            -The input text file 1 contains MOI of each frame as the 8th attribute
            -The input text file 2 contains SOM of each frame as the 7th attribute
            -We push the data into an RDD
            -collect required data from RDD into an Array using ".collect" method
            -Compute the total center of mass using reduce(_+_) method for each frame
         */
  def main(args: Array[String]) {
        val logFile1 = "./output/MOI.txt"       /* Should be the path of some file on your system*/
        val logFile2 = "./output/SOM.txt"
        val conf = new SparkConf().setAppName("MS Data") 
        val sc = new SparkContext(conf)            /* create a new spark context */
        var str = ""
        var comRDD = Array.ofDim[Float](101)
        val start_time = Calendar.getInstance().getTime()
        str += "Starting Time %s\n".format(start_time)
        val moiRDD = sc.textFile(logFile1,2).filter(line => line.contains("Frame")).map(_.split(" ")(7)).map(_.toFloat).collect  //get  MOI 
        val somRDD = sc.textFile(logFile2,2).filter(line => line.contains("Frame")).map(_.split(" ")(6)).map(_.toFloat).collect  //get  SOM
                         /*filtering lines containing "Frame" and getting only Required columns*/  /*Collecting data into an Array[Float] */
                                                
        var i = 0
        for (i <-0 to moiRDD.length-1)                           /*For all the frames*/
        {  
                comRDD(i) = moiRDD(i)/somRDD(i)           //compute Center of Mass = MOI/SOM                                                    
        	println("Frame %s -> Center of Mass : %f\n".format(i,comRDD(i)))
       		str += "Frame %s -> Center of Mass : %f\n".format(i,comRDD(i))     
	}
        val end_time = Calendar.getInstance().getTime()
        str += "Ending Time %s".format(end_time)
        printToFile(str)                          /*store the output to a text file */                 
  }
 
    /* - We have defined a function "prinToFile" to write the output to a text file 
       - It takes a string and a location as input
       - Writes the contents of the input string to the text file in the specified location
       */
  def printToFile(content: String, location: String =  "./output/COM.txt") =
  Some(new java.io.PrintWriter(location)).foreach{f => try{f.write(content)}finally{f.close}} 
}
																																																																																																																
