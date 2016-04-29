/*import java.io.IOException;
import java.util.*;
import java.lang.Object;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class HighestMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable>
{
		public static final int MISSING = 9999;
		
     
            public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException
      {
            	

            String line = value.toString();
            String year = line.substring(14,18);
            String placeYear = "";
            
            if(line.substring(0, 6).equals("037720")){
            	placeYear = "London";
            } else if(line.substring(0, 5).equals("071500")){
            	placeYear = "Paris";
            } else if(line.substring(0, 5).equals("082210")){
            	placeYear = "Madrid";
            } else if(line.substring(0, 5).equals("417800")){
            	placeYear = "Karachi";
            } else if(line.substring(0, 5).equals("421820")){
            	placeYear = "New Delhi";
            } else if(line.substring(0, 5).equals("471100")){
            	placeYear = "Seoul";
            } else if(line.substring(0, 5).equals("476710")){
            	placeYear = "Tokyo";
            } else if(line.substring(0, 5).equals("545110")){
            	placeYear = "Beijing";
            } else if(line.substring(0, 5).equals("623660")){
            	placeYear = "Cairo";
            } else if(line.substring(0, 5).equals("716240")){
            	placeYear = "Toronto";
            } else if(line.substring(0, 5).equals("722950")){
            	placeYear = "Los Angeles";
            } else if(line.substring(0, 5).equals("723860")){
            	placeYear = "Las Vegas";
            } else if(line.substring(0, 5).equals("725090")){
            	placeYear = "Boston";
            } else if(line.substring(0, 5).equals("725030")){
            	placeYear = "New York City";
            } else if(line.substring(0, 5).equals("725300")){
            	placeYear = "Chicago";
            } else if(line.substring(0, 5).equals("724050")){
            	placeYear = "Washington DC";
            } else if(line.substring(0, 5).equals("766793")){
            	placeYear = "Mexico City";
            } else if(line.substring(0, 5).equals("837550")){
            	placeYear = "Rio de Janeiro";
            } else if(line.substring(0, 5).equals("947670")){
            	placeYear = "Sydney";
            } else if(line.substring(0, 5).equals("984290")){
            	placeYear = "Manila";
            }
            
            placeYear += " " + year;
            String monthNDay = line.substring(18, 22);
            String tempMax = line.substring(102, 108);
            String tempMin = line.substring(110, 116);
            String totalPrecip = line.substring(118, 123);
            String windSpeed = line.substring(88, 93);
            if(line.substring(57, 63).equals("9999.9")){
            	
            }
            else{	
            		String pressure = line.substring(57, 63);
            	}
            
            
            
           
            Float temperature;
           if (line.charAt(0)!='S' ){
           
                        temperature = (Float.parseFloat((line.substring(26, 31)).trim()));
                 
            String quality = line.substring(92, 93);
            if(temperature != MISSING && quality.matches("[01459]"))
            output.collect(new Text(year),new FloatWritable(temperature));
           }   
}
       
}

*/

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class HighestMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
		public static final double MISSING = 9999.9;
	
     
            public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
      {
            String line = value.toString();
            String year = line.substring(15,19);
            String month = line.substring(19,21);
            String day = line.substring(21,23);
           // String station = line.substring(4,10);
            String placeYear = "";
            
            if(line.substring(4, 10).equals("037720")){
            	placeYear = "London";
            } else if(line.substring(4, 10).equals("071500")){
            	placeYear = "Paris";
            } else if(line.substring(4, 10).equals("082210")){
            	placeYear = "Madrid";
            } else if(line.substring(4, 10).equals("417800")){
            	placeYear = "Karachi";
            } else if(line.substring(4, 10).equals("421820")){
            	placeYear = "New Delhi";
            } else if(line.substring(4, 10).equals("471100")){
            	placeYear = "Seoul";
            } else if(line.substring(4, 10).equals("476710")){
            	placeYear = "Tokyo";
            } else if(line.substring(4, 10).equals("545110")){
            	placeYear = "Beijing";
            } else if(line.substring(4, 10).equals("623660")){
            	placeYear = "Cairo";
            } else if(line.substring(4, 10).equals("716240")){
            	placeYear = "Toronto";
            } else if(line.substring(4, 10).equals("722950")){
            	placeYear = "Los Angeles";
            } else if(line.substring(4, 10).equals("723860")){
            	placeYear = "Las Vegas";
            } else if(line.substring(4, 10).equals("725090")){
            	placeYear = "Boston";
            } else if(line.substring(4, 10).equals("725030")){
            	placeYear = "New York City";
            } else if(line.substring(4, 10).equals("725300")){
            	placeYear = "Chicago";
            } else if(line.substring(4, 10).equals("724050")){
            	placeYear = "Washington DC";
            } else if(line.substring(4, 10).equals("766793")){
            	placeYear = "Mexico City";
            } else if(line.substring(4, 10).equals("837550")){
            	placeYear = "Rio de Janeiro";
            } else if(line.substring(4, 10).equals("947670")){
            	placeYear = "Sydney";
            } else if(line.substring(4, 10).equals("984290")){
            	placeYear = "Manila";
            }
            
            double maxtemperature;  
            double mintemperature;
            double maxpressure;
            if (line.charAt(93)=='+'){
                        maxtemperature = Double.parseDouble(line.substring(88, 92));
                        maxtemperature /= 10;
            			mintemperature = Double.parseDouble(line.substring(94, 98));
            			mintemperature /= 10;
            }
            else {
                       maxtemperature = Double.parseDouble(line.substring(87, 92));
                       maxtemperature /= 10;
                       mintemperature = Double.parseDouble(line.substring(93, 98));
                       mintemperature /= 10;
            }
            
            maxpressure = Double.parseDouble(line.substring(99,104));
            maxpressure /= 10;
        
            String quality = line.substring(92, 93);
            if(mintemperature != MISSING && quality.matches("[01459]") && maxtemperature != MISSING  && maxpressure != MISSING )
            output.collect(new Text(year+"_"+month+"_"+day+"_"+placeYear),
            		new Text(placeYear +","+month+"-"+day +","+maxtemperature +","+mintemperature + "," + maxpressure));
      /*      
            if(maxtemperature != MISSING && quality.matches("[01459]"))
                output.collect(new Text(year+"_"+month+"_"+day+"_"+placeYear),new IntWritable(maxtemperature));
              
            
            if(pressure != MISSING && quality.matches("[01459]"))
            output.collect(new Text(year+"_"+month+"_"+day+"_"+placeYear),new IntWritable(pressure));
          */  
            
            
            
            }
       
}
	
	
	
	                   
