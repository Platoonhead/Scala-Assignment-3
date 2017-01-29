/*
    println is a side effect function and must not be used ,
    still its been used here to format the output for DEMOSTRATION PURPOSE ONLY  
*/
import java.io._
trait Extract{

        // function to extract content of file and retun it as a String
	def extractFileDataFrom(fileName:File):String={

                try scala.io.Source.fromFile(fileName).mkString
                finally scala.io.Source.fromFile(fileName).close
	}

        // function to get names of all .txt files exist in the provided directory
        def getAllTxtFileNames(directory:String):List[File]={

		val atDirectory = new File(directory)
                if(atDirectory.exists && atDirectory.isDirectory){

		val allFiles = atDirectory.listFiles.filter(_.isFile)
	        //get all txt files, file name converted to string using "" so as to apply filter
		val txtFiles = allFiles.toList.filter{filename=>(filename+"").split('.')((filename+"").split('.').toList.size-1)=="txt"} 
		txtFiles
        
                }
                else List() // no files in directory 
        }

	def extractFromDatabase{} // blank implementation, reserved for future requirements

}
//________________________________________________________________________________
class Transform(source: String,destination: String) extends Extract with Load{

        val listOfFiles = getAllTxtFileNames(source) // list of all the .txt files in source directory provided

       //function convert file content to uppercase
        def CapatalizeContent:Boolean={

                if(!listOfFiles.isEmpty){
             
                 val processedData = for{
                                        filename <- listOfFiles
                                        }yield loadToFile(extractFileDataFrom(filename).toUpperCase,destination,filename) 
                                         //read file and convert to capital
                                         //to use SQL,invoke "extractFromDatabase" instead of extractFileDataFrom
                                         // to store data to database use loadToDatabase instead of loadToFile function
		         if(processedData.contains(false))false
		         else true

                }
                else false
  
         }

        //function to count the frequency of each unique word in the source file
         def frequencyOfWords:Boolean={


		if(!listOfFiles.isEmpty){
			     
                val processedData = for{
			   filename <- listOfFiles
                           //to use SQL,invoke "extractFromDatabase" instead of extractFileDataFrom
                           wordsList = extractFileDataFrom(filename).toLowerCase.replaceAll("\n"," ").split(' ').toList
                           wordsMappedWithFrequency = wordsList map (word => "\n" + word -> wordsList.groupBy( _ == word )(true).size)
		           }yield loadToFile( wordsMappedWithFrequency.toMap.mkString,destination,filename)
                           // to store data to database use loadToDatabase instead of loadToFile function
		         if(processedData.contains(false))false
		         else true

		}
		else false
      
        } 

}
//_________________________________________________________________________________
trait Load{

        //function to create a file with same name,populate data to it and store it in destination directory
	def loadToFile(infoToStore : String,destination : String ,name : File):Boolean={
              	
                val atDirectory = new File(destination)
                if(!atDirectory.exists && !atDirectory.isDirectory)atDirectory.mkdir //create directory if it dosent exists
                try{ new PrintWriter(destination+"/"+name.toString.split('/')(1)) { write(infoToStore); close };true}
                catch{case ex: IOException => println(ex);false}	  
        }

        def loadToDatabase{} // blank implementation, reserved for future requirements

}
//_________________________________________________________________________________

object FileProcessing extends App{

  println("\n\n_______________________ETL Application REPORT___________________________________________\n\n")
  val definedTargets = new Transform("filesource","capitalize") //source and destination directory name as parameter
  val result = definedTargets.CapatalizeContent
  println("TRANSFORMATION APPLIED : Capitalize the content")
  if(result)println("RESULT : SUCESSFUL\n") else println("RESULT : FAILED\nPOSSIBLE REASONS : Directory DOSENT EXISTS or No files in directory to read\n\n")


  val definedTargets2 = new Transform("filesource","wordcount") //source and destination directory name as parameter
  val result2 = definedTargets2.frequencyOfWords
  println("TRANSFORMATION APPLIED : Count frequency of each unique word")
  if(result2)println("RESULT : SUCESSFUL\n") else println("RESULT : FAILED\nPOSSIBLE REASONS : Directory DOSENT EXISTS or No files in directory to read\n")

  println("\n_________________________________________________________________________________________\n\n")
}
