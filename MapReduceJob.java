
/* Author : Anbalagan Mookkaiah
	/* Map Reduce Job contains collection map tasks, and reducers.  Combiner is mixed wiht Mapper for local aggregation. This class is responsible for
	 * 1. Iterating through each input file. 
	 * 2. Divides the contents of each file based on the given file block size.
	 * 3. Allocate  thread pool based on the given count.
	 * 4. Schedules Map tasks based on the Input File Split. 
	 * 5. Shuffle and Sort output from all map tasks to generate consolidated output.
	 * 6. Based on number of keys available, schedules reducer tasks.
	 * 7. Generate the consolidated output from all reducer task output.
	 	 */
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

					

	 public  class MapReduceJob
	{
		private String jobID;
		private String inputPath;
		private String outputPath;
		private Integer threadCount;
		private Integer fileBlockSize;
		private Integer reattemptCount;
		private Integer timeoutInSeconds;
		private TreeMap<String,String> consolidetdMapOuput;
		private TreeMap<String,Integer> consolidatedReducerOutput;
		
		public MapReduceJob(String jobID)
		{
			this.jobID = jobID;
		}
		
		public void cofigureJob(String inputPath, String outputPath, Integer threadCount,Integer fileBlockSize,Integer reattemptCount, Integer timeoutInSeconds)
		{
			this.inputPath = inputPath;
			this.outputPath = outputPath;
			this.threadCount = threadCount;
			this.fileBlockSize = fileBlockSize;
			this.reattemptCount = reattemptCount;
			this.timeoutInSeconds = timeoutInSeconds;
		}
		/*
		 * Schedules map tasks
		 * Runs the reducer and generates output.
		 */
		public void generateWordCount()
		{
			try
			{
			  this.scheduleMapTasks(this.getInputFiles());          //gets the sorted input files
			  this.schduleReducer();
			  this.writeIntoOutputFile();
			}
			 catch (IOException ex) 
	         {
	            ex.printStackTrace();
	         } 
			catch (Exception ex) 
	         {
	            ex.printStackTrace();
	         } 
		        
		}
		private void scheduleMapTasks(File[] directoryListing) throws IOException
		{
			
			 /*TODO: Task scheduler should be written to handle the task..
			  * Due to time constraint I'm using inbuilt executor service*/
			 ArrayList<String> tempFolderList = new ArrayList<String>();
			try
			{
			 
			  // Create a thread pool with the requested number
			   ExecutorService pool = Executors.newFixedThreadPool(this.threadCount);
			   	Mapper workers[] = new Mapper[this.threadCount];
			     @SuppressWarnings("unchecked")
				Future<TreeMap<String,Integer>>[] futures = new Future[this.threadCount];
			   Integer index = 0 ;
				
			 	   for (File child : directoryListing) 
			   
				 { 
					   String folderName = "File" + (index + 1);
			 		   File tempFolder = new File(folderName);
			 		  tempFolder.mkdir();
					   tempFolderList.add(folderName);
					   FileSpliter fileSpilit = new FileSpliter();
						 List<String> fileNames = fileSpilit.splitFile(child, this.fileBlockSize, folderName);
						 // For each defined block size, create one MAP task.
						 for(String fileName: fileNames)
						 {
							 workers[index] =  new Mapper(UUID.randomUUID().toString(),  fileName);
					         futures[index] = pool.submit(workers[index]);
							
						 }
					     index++;
				 }
				    /* Wait for all map tasks to complete before starting reducer 
				     * Additional error handling is required to make sure values are returned from all threads.
				     * **/
			 	  consolidetdMapOuput = new TreeMap<String,String> ();
				      for (int i = 0; i < this.threadCount; ++i)
				      {
				         	this.shuffleAndSort(futures[i].get());
				        
				      }
				      
				     }
					 catch (InterruptedException ex) 
			         {
			            ex.printStackTrace();
			         } catch (ExecutionException ex)
			         {
			            ex.printStackTrace();
			         }
			
				     finally
				     {
				    	 for (String fileName : tempFolderList) 
				    		 (new File(fileName)).delete();
				     }
			 }
		
		//This method returns sorted file array from the right directory. 
	    private File[] getInputFiles ()
	    {
	        File dir = new File(this.inputPath); //location of directory with input file(s)
	        File[] directoryListing = dir.listFiles(); //lists all the files
	        Arrays.sort (directoryListing); //sorts all the listed files by name within the directory
	        return directoryListing;
	    }
	    
	    private void writeIntoOutputFile() throws FileNotFoundException 
	    {
	    PrintWriter outWriter = new PrintWriter( new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.outputPath))));
	      try
	      {
	            //"wc_output/wc_result.txt"; //Folder location for output file
	            for(Object key: this.consolidatedReducerOutput.keySet())
	            {
	                outWriter.print(key + "\t" + consolidatedReducerOutput.get(key) + "\n"); //Prints sorted key, value to output file. TreeMap sorts automatically (req 5)
	            }            
	            outWriter.flush();
	        }
	      finally
	      {
	    	  if (outWriter != null )
	    	  outWriter.close();
	      }
	      
	    }
	    	    
	    private void shuffleAndSort(TreeMap<String,Integer> mapOutput)
	    {  
	    	for(Map.Entry<String,Integer> entry : mapOutput.entrySet()) 
	    	{
	    		  String key = entry.getKey();
	    		  Integer value = entry.getValue();

	    		  if(consolidetdMapOuput.containsKey(key)) 
	              {              
	                    String count = consolidetdMapOuput.get(key);      //Assigns words as an integer (for counting)
	                    consolidetdMapOuput.put(key,  count + "," + value.toString());   //increaments value if already exists
	                } 
	    		  else 
	    			  consolidetdMapOuput.put(key, key.toString()); 
	    		}
	    	
	    }
	    
	    private void consolidateReducerOutput(HashMap<String,Integer> mapOutput)
	    {  
	    	for(Map.Entry<String,Integer> entry : mapOutput.entrySet()) 
	    	{
	    		  String key = entry.getKey();
	    		  Integer value = entry.getValue();

	    		  if(consolidatedReducerOutput.containsKey(key)) 
	              {              
	                    Integer count = consolidatedReducerOutput.get(key);      //Assigns words as an integer (for counting)
	                    consolidatedReducerOutput.put(key,  count +  value);            //increaments value if already exists
	                } 
	    		  else 
	    			  consolidatedReducerOutput.put(key, value); 
	    		}
	    	
	    }
	    
	    private void schduleReducer()
	    {	    	
			try
			{
			  // Create a thread pool with the requested number
			   ExecutorService pool = Executors.newFixedThreadPool(consolidetdMapOuput.size());
			   	Reducer workers[] = new Reducer[consolidetdMapOuput.size()];
			     @SuppressWarnings("unchecked")
				Future<HashMap<String,Integer>>[] futures = new Future[consolidetdMapOuput.size()];
			     Set<String> keys = consolidetdMapOuput.keySet();
			     Integer index = 0;
			     /* Create reducer jobs as many as unique keys */
			        for(String key: keys)
			        {		 
			        	workers[index] =  new Reducer(key,  consolidetdMapOuput.get(key));
					    futures[index] = pool.submit(workers[index]);
						 index++;
			        }
				    /* Wait for all reducer tasks to complete to write into output file 
				     * Additional error handling is required to make sure values are returned from all threads.
				     * **/
			 	  
			        consolidatedReducerOutput = new TreeMap<String,Integer> ();
				      for (int i = 0; i < consolidetdMapOuput.size(); ++i)
				      {
				         
				        	this.consolidateReducerOutput(futures[i].get());
				         			       
				      }
				      
				     }
			 catch (ExecutionException ex)
	         {
	            ex.printStackTrace();
	         }
				      catch(InterruptedException ex)
				      {
				    	  ex.printStackTrace();
				      }
				   
			 }
	    }

