package consumers;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.commons.math3.stat.regression.SimpleRegression;

public class SimpleRegressionImpl {

	private static String day = "";
	private static double tempMax = 0;
	private static double tempMin = 0;
	private static double preMax = 0;
	private static double[][] tempMaxArr = new double[9855][9855];
	private static double[][] tempMinArr = new double[9855][9855];
	private static double[][] preMaxArr = new double[9855][9855];
	public static double forecastTMax = 0;
	public static double forecastTMin = 0;
	public static double forecastPress = 0;
	
	// y = α + ßx ==> α = y-intercept, ß=slope
	public static double alphaTmax = 0;
	public static double betaTmax = 0;
	public static double alphaTmin = 0;
	public static double betaTmin = 0;
	public static double alphaPress = 0;
	public static double betaPress = 0;
	
	
	// create SLR model
	private static SimpleRegression simpleRegressionTmpMax = new SimpleRegression(true);
	private static SimpleRegression simpleRegressionTmpMin = new SimpleRegression(true);
	private static SimpleRegression simpleRegressionPreMax = new SimpleRegression(true);
	
	private static String path = "/opt/WeatherEvents/part-00000";
	
	public static void main(String[] args) throws IOException {
		
	}
	
	public SimpleRegressionImpl() throws NumberFormatException, IOException{
		
		ArrayList<String> zones = new ArrayList<String>();
		zones.add("Las Vegas");
        zones.add("Mexico City");
        zones.add("Toronto");
        zones.add("New York City");
        zones.add("Karachi");
        zones.add("New Delhi");
        zones.add("Manila");
        zones.add("Sydney");
        zones.add("Seoul"); 
        zones.add("Tokyo");
        zones.add("Cairo");
        zones.add("Los Angeles");
        zones.add("Rio de Janeiro");
        zones.add("Washington DC");
        zones.add("Beijing");
        zones.add("Chicago"); 
        zones.add("London");
        zones.add("Paris"); 
        zones.add("Madrid"); 
        zones.add("Boston");
        ArrayList<String> lines = new ArrayList<String>();
		for(String s : zones){
			setData(1, 5, s);
			
			lines.add(s+","+alphaTmax+","+alphaTmin+","+alphaPress+","+
					betaTmax+","+betaTmin+","+betaPress);
		}
		Path file = Paths.get("/opt/WeatherEvents/equation_params");
		
		Files.write(file, lines, Charset.forName("UTF-8"));
		
	}	
	
	public void setData(int day, int month, String place) throws NumberFormatException, IOException{
		
		FileInputStream fis = new FileInputStream(path);
		BufferedReader in = new BufferedReader(new InputStreamReader(fis));
		
		String line;
		double inpDay = 0; 
		
		   if(month == 1){
			   inpDay = day; //day of the year
		   } else if(month == 2){
			   inpDay = day+ 31; //day of the year
		   } else if(month == 3){
			   inpDay = day + 59; //day of the year
		   } else if(month == 4){
			   inpDay = day + 90; //day of the year
		   } else if(month == 5){
			   inpDay = day + 120; //day of the year
		   } else if(month == 6){
			   inpDay = day + 151; //day of the year
		   } else if(month == 7){
			   inpDay = day + 181; //day of the year
		   } else if(month == 8){
			   inpDay = day + 212; //day of the year
		   } else if(month == 9){
			   inpDay = day + 242; //day of the year
		   } else if(month == 10){
			   inpDay = day + 273; //day of the year
		   } else if(month == 11){
			   inpDay = day + 303; //day of the year
		   } else if(month == 12){
			   inpDay = day + 334; //day of the year
		   }
		
		
		int count = 0;
		
	    while ((line = in.readLine()) != null) {
	    	//add each day to the model
	       if(line.split("	")[0].substring(11).equals(place)){
	    		   double x = Double.parseDouble(line.substring(8, 10));
	    		   
	    		   if(x == 1){
	    			    //day of the year
	    		   } else if(x == 2){
	    			   x = x + 31; //day of the year
	    		   } else if(x == 3){
	    			   x = x + 59; //day of the year
	    		   } else if(x == 4){
	    			   x = x + 90; //day of the year
	    		   } else if(x == 5){
	    			   x = x + 120; //day of the year
	    		   } else if(x == 6){
	    			   x = x + 151; //day of the year
	    		   } else if(x == 7){
	    			   x = x + 181; //day of the year
	    		   } else if(x == 8){
	    			   x = x + 212; //day of the year
	    		   } else if(x == 9){
	    			   x = x + 242; //day of the year
	    		   } else if(x == 10){
	    			   x = x + 273; //day of the year
	    		   } else if(x == 11){
	    			   x = x + 303; //day of the year
	    		   } else if(x == 12){
	    			   x = x + 334; //day of the year
	    		   }
	    		   
	    		   double yMax = Double.parseDouble(line.split("	")[1].split(",")[2]); //value measure
	    		   double yMin = Double.parseDouble(line.split("	")[1].split(",")[3]);
	    		   double yPre = Double.parseDouble(line.split("	")[1].split(",")[4]);
	    		  
	    		   tempMaxArr[count][0] = x;
	    		   tempMinArr[count][0] = x;
	    		   preMaxArr[count][0] = x;
	    		   tempMaxArr[count][1] = yMax;
	    		   tempMinArr[count][1] = yMin;
	    		   preMaxArr[count][1] = yPre;   
	    		   count++;
	    		   
	    	   
	       }
	    }
	    
	    // add data to the model
	    simpleRegressionTmpMax.addData(tempMaxArr);
	    simpleRegressionTmpMin.addData(tempMinArr);
	    simpleRegressionPreMax.addData(preMaxArr);
	    
	    alphaTmax = simpleRegressionTmpMax.getIntercept();
	    betaTmax = simpleRegressionTmpMax.getSlope();
	    alphaTmin = simpleRegressionTmpMin.getIntercept();
	    betaTmin = simpleRegressionTmpMin.getSlope();
	    alphaPress = simpleRegressionPreMax.getIntercept();
	    betaPress = simpleRegressionPreMax.getSlope();
	    
	    //forecastTMax = simpleRegressionTmpMax.predict(inpDay);
//	    forecastTMax = alpha + inpDay*beta;
//	    forecastTMin = simpleRegressionTmpMin.predict(inpDay);
//	    forecastPress = simpleRegressionPreMax.predict(inpDay);   
		
	}
	
}
