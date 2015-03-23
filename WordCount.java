
/* Author : Anbalagan Mookkaiah
 *
 * This application is designed to run on a single node environment. 
 * Based on the target node's configuration (RAM, Processor), this program expects the 
 * maximum thread count and the file block size from the administrator who runs this program.
 * Following input parameters are required for configuration.
 *   Input Path, Output Path, Max Thread Count, File Block Size, Reattempt and Timeout.
 *   
 *   Application Design
 *   ======
 *   1. One MapReduceJob Class
 *   2. Mapper Class
 *   3. Reducer Class
 *   
 *   MapReduceJob is responsible for overall process that includes
 *    * 1. Iterating through each input file. 
	 * 2. Divides the contents of each file based on the given file block size.
	 * 3. Allocate  thread pool based on the given count.
	 * 4. Schedules Map tasks based on the Input File Split. 
	 * 5. Shuffle and Sort output from all map tasks to generate consolidated output.
	 * 6. Based on number of keys available, schedules reducer tasks.
	 * 7. Generate the consolidated output from all reducer task output.
	 *
 * Mapper goes through the given key value pair and returns key value pair with the locally aggregated value ( as in in-line combiner) to avoid 
 * more data to be transferred across network.
  * Reducer goes through given key value pair and returns final key value pair.
 *  Due to my workload and time constraint, I couldnot consentrate more on the following things which I would generally do as programmer. 
 *  If I am given chance again, I would fine tune my code to make it more robust.
  *  More logging and error handling to monitor the  thread and try to reattempt or kill after timeout.
 *  While reading text files by block size, there is a possibility one single word is broken into two..
 *  Due to time constraint, I am not able to complete this.  If I am given a chance again, I will do it..
 */

import java.util.UUID;


public class WordCount
{
	 		public static void main(String[] args) 
	{      
		/*String INPUT_FILEPATH =  args[0];
	    String OUTPUT_FILEPATH = args[1];
        Integer THREAD_COUNT =  Integer.parseInt(args[2]);  // Maximum number of threads allowed for this process 
        Integer FILE_BLOCK_SIZE_IN_BYTES =  Integer.parseInt(args[3]); // File block size for each map task
        Integer REATTEMPT_COUNT =  Integer.parseInt(args[4]);
        Integer TIMEOUT_IN_SECONDS =  Integer.parseInt(args[5]); */
			String INPUT_FILEPATH =  "wc_input";
		    String OUTPUT_FILEPATH = "wc_output/wc_result.txt";
	        Integer THREAD_COUNT =  10;
	        Integer FILE_BLOCK_SIZE_IN_BYTES =  10;
	        Integer REATTEMPT_COUNT =  10;
	        Integer TIMEOUT_IN_SECONDS =  1000;
                       
        // Create new MapReduce Job
        
	     MapReduceJob mapReduceJobObject = new MapReduceJob(UUID.randomUUID().toString());
        mapReduceJobObject.cofigureJob(INPUT_FILEPATH, OUTPUT_FILEPATH, THREAD_COUNT, FILE_BLOCK_SIZE_IN_BYTES, REATTEMPT_COUNT, TIMEOUT_IN_SECONDS);
        mapReduceJobObject.generateWordCount();
     }
}			
	