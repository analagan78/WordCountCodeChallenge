
import java.util.HashMap;

import java.util.List;

import java.util.Arrays;

import java.util.concurrent.Callable;

	public class Reducer implements Callable<HashMap<String, Integer>>
	{
	
	   private String key;
	   private String values;
	   HashMap<String, Integer> dictionary;
		   
	   /* Constructor creates the thread and starts it*/
	   Reducer(String key,String values)
	   { 
	       
		   this.key = key;
		   this.values = values;
	   }
	   
	   public HashMap<String, Integer> Reduce(String key, String valueList)
	   {   
		   List<String> values = Arrays.asList(valueList.split(","));
		   
					 dictionary.put(key, 0);
		 
		   for (String value: values)
		   {
			   
			   dictionary.put(key,dictionary.get(key)+ Integer.parseInt(value));
		   }
		   return  dictionary;
		  
	   }
	   
	   public HashMap<String, Integer> call()
	   {

	    		try
	    		{
	    			dictionary = new HashMap<String, Integer>();
	    			this.Reduce(this.key, this.values);
	    			
	    		}
	    		catch (Exception ex)
	    		{
	    			ex.printStackTrace();
	    		}
	    	    	
	    		return dictionary;
	     
	   }
	   
	   
	}
  

