/*import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class HighestReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable>
{
      
      public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException
      {
    	  Float max_temp = (float) 0.0; 
    	  ; 
          while (values.hasNext())
                      {
        	  Float current=values.next().get();
                         if ( max_temp <  current)  
                        	 max_temp =  current;
                      }
          output.collect(key, new FloatWritable(max_temp));

      }	
      
}

*/

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class HighestReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{
      
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
      {
    	  double max_temp = 0; 
    	  double min_temp = -200;
    	  double max_pressure = 0;
    	  String place = "";
    	  String monthDay = "";
    	  
    	//  String line = values.toString();
       /*   while (values.hasNext())
                      {
        	  int temax=values.s;
                         if ( max_temp <  current)  
                        	 max_temp =  current;
                      }
          output.collect(key, new IntWritable(max_temp));
         */ 
         
          
          while(values.hasNext()){
        	  String temp = values.next().toString();
        	  
        	  	place = temp.split(",")[0];
        	  	monthDay = temp.split(",")[1];
        	  	double currentTMax = Double.parseDouble(temp.split(",")[2]);
        	  	double currentTMin = Double.parseDouble(temp.split(",")[3]);			
        	  	double currentPmax = Double.parseDouble(temp.split(",")[4]);
        	  	
        	  	if (max_temp < currentTMax){
        	  		max_temp = currentTMax;
        	  	}
        	  	if (min_temp < currentTMin){
        	  		min_temp = currentTMin;
        	  	}
        	  	if (max_pressure < currentPmax){
        	  		max_pressure = currentPmax;
        	  	}
          }
          output.collect(key, new Text(place + "," + monthDay + "," + max_temp + "," + min_temp +","+ max_pressure));

      }

	
		
	
      
}
      
      
      
      
      
     
