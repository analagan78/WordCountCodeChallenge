
	/*
	 * Mapper parses words in each given line.
	 * Thread priority is left at default which is 4 (Normal)
	 * Mapper also does the local aggregation to avoid the more data to be passed acrorss network.
	 */


import java.io.*;

import java.util.TreeMap;
import java.util.StringTokenizer;

import java.util.concurrent.Callable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import java.io.IOException;

	public class Mapper implements Callable<TreeMap<String, Integer>>
	{
	   
	   private String taskID;
	   private String sourceFile;
	    TreeMap<String, Integer> dictionary;
	   /* Constructor creates the thread and starts it*/
	    Mapper(String taskID,String sourceFile)
	   { 
	      
		   this.taskID = taskID;
		   this.sourceFile = sourceFile;
	   }
	   
	   public TreeMap<String, Integer> Map(String key, String value)
	   {   
		   try
		   {
			   StringTokenizer tokenizer = new StringTokenizer(value);
			 
			   while (tokenizer.hasMoreTokens())
			   {
				  String word = tokenizer.nextToken().replaceAll("[^a-zA-Z]+",""); //replaces any character other than letters
	              if(dictionary.containsKey(word)) 
	              {              
	                    int count = (int)dictionary.get(word);      //Assigns words as an integer (for counting)
	                    dictionary.put(word, count + 1);            //increaments value if already exists
	                } else 
	                	dictionary.put(word, 1);       
			   }
			  
		   }
		    catch(Exception  e)
		   {
		    	e.printStackTrace();
		   }
		return dictionary;
		  
	   }
	   
	   public TreeMap<String, Integer> call() throws FileNotFoundException, IOException
	   {
		   FileInputStream fstream = new FileInputStream(this.sourceFile);
		   BufferedReader bReader = new BufferedReader(new InputStreamReader(fstream));
		   dictionary = new TreeMap<String, Integer>();
	      try
	      
	      {
	    	  String strLine;
     
	    	  //Read File Line By Line
	    	  while ((strLine = bReader.readLine()) != null)   {
	    		  this.Map("TaskID" , strLine);
	    	 
	    	  }

	    	  return dictionary;
	      }
	      catch(FileNotFoundException ex)
	      {
	    	  ex.printStackTrace();
	      }
	      catch(IOException ex)
	      {
	    	  ex.printStackTrace();
	      }
	    
	     finally
	     {
	    	 bReader.close();
	    	 fstream.close();
	    	 (new File(this.sourceFile)).delete();
	    	 
	     }
	      return dictionary;
	   }
	   
	   
	}
	
  

