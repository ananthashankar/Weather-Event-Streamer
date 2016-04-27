package consumers;

import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WeatherScheme implements Scheme 
{
		public static final String FIELD_PLACE = "place";
		public static final String FIELD_LATITUDE = "latitude";
		public static final String FIELD_LONGITUDE = "longitude";
		public static final String FIELD_DAY = "day";
		public static final String FIELD_SUMMARY = "summary";
		public static final String FIELD_PRECIPPROBABILITY = "precipProbability";
		public static final String FIELD_PRECIPINTENSITY = "precipIntensity";
		public static final String FIELD_TEMPERATUREMAX = "temperatureMax";
		public static final String FIELD_TEMPERATUREMIN = "temperatureMin";
		public static final String FIELD_ICON = "icon";
		public static final String FIELD_CLOUDCOVER = "cloudCover";
		public static final String FIELD_OZONE = "ozone";
		public static final String FIELD_HUMIDITY = "humidity";
		public static final String FIELD_WINDSPEED = "windSpeed";
		public static final String FIELD_MOONPHASE = "moonPhase";
		public static final String FIELD_WINDBEARING = "windBearing";
		public static final String FIELD_PRESSURE = "pressure";
		public static final String FIELD_TIME = "time";
        
        
	private static final long serialVersionUID = -2990121166902741545L;

	private static final Logger LOG = Logger.getLogger(WeatherScheme.class);
	
        /**
         * <timestamp>|truckid|driverId|eventType|long|lat
         * @param bytes
         * @return 
         */
	@Override
	public List<Object> deserialize(byte[] bytes)
        {
		try 
                {
			String weatherEvent = new String(bytes, "UTF-8");
			String[] pieces = weatherEvent.split("\n");
			String place = "";
			String latitude = "";
			String longitude = "";
			String day = "";
			String summary = "";
			String precipProbability = "";
			String precipIntensity = "";
			String temperatureMax = "";
			String temperatureMin = "";
			String icon = "";
			String cloudCover = "";
			String ozone = "";
			String humidity = "";
			String windSpeed = "";
			String moonPhase = "";
			String windBearing = "";
			String pressure = "";
			String time = "";
			
			for(int i=0; i<pieces.length; i++){
				if(i == 0 && pieces[0].length() > 0){
					place = pieces[i];
				}
				else if(pieces[i].split(":")[0].equals("latitude")){
					latitude = pieces[i].split(":")[1];
				} 
				else if(pieces[i].split(":")[0].equals("longitude")){
					longitude = pieces[i].split(":")[1];
				} 
				else if(pieces[i].substring(0, 3).equals("Day")){
					day = pieces[i].substring(0, 6);
				} 
				else if(pieces[i].split(":")[0].equals("summary")){
					summary = pieces[i].split(":")[1];
				}
				else if(pieces[i].split(":")[0].equals("precipProbability")){
					precipProbability = pieces[i].split(":")[1];
				}
				else if(pieces[i].split(":")[0].equals("precipIntensity")){
					precipIntensity = pieces[i].split(":")[1];
				}
				else if(pieces[i].split(":")[0].equals("temperatureMax")){
					temperatureMax = pieces[i].split(":")[1];
				}
				else if(pieces[i].split(":")[0].equals("temperatureMin")){
					temperatureMin = pieces[i].split(":")[1];
				}
				else if(pieces[i].split(":")[0].equals("icon")){
					icon = pieces[i].split(":")[1];
				}
				else if(pieces[i].split(":")[0].equals("cloudCover")){
					cloudCover = pieces[i].split(":")[1];
				}
				else if(pieces[i].split(":")[0].equals("ozone")){
					ozone = pieces[i].split(":")[1];
				}
				else if(pieces[i].split(":")[0].equals("humidity")){
					humidity = pieces[i].split(":")[1];
				}
				else if(pieces[i].split(":")[0].equals("windSpeed")){
					windSpeed = pieces[i].split(":")[1];
				}
				else if(pieces[i].split(":")[0].equals("moonPhase")){
					moonPhase = pieces[i].split(":")[1];
				}
				else if(pieces[i].split(":")[0].equals("windBearing")){
					windBearing = pieces[i].split(":")[1];
				}
				else if(pieces[i].split(":")[0].equals("pressure")){
					pressure = pieces[i].split(":")[1];
				}
				else if(pieces[i].split(":")[0].equals("time")){
					pressure = pieces[i].split(":")[1];
				}
				
			}

			return new Values(cleanup(place), cleanup(day), cleanup(latitude), cleanup(longitude), cleanup(summary), cleanup(precipProbability), 
					cleanup(precipIntensity), cleanup(temperatureMax), cleanup(temperatureMin),
					cleanup(icon), cleanup(cloudCover), cleanup(ozone), cleanup(humidity),
					cleanup(windSpeed), cleanup(moonPhase), cleanup(windBearing),
					cleanup(pressure), cleanup(time));
			
		} 
                catch (UnsupportedEncodingException e) 
                {
                    LOG.error(e);
                    throw new RuntimeException(e);
		}
		
	}
        
	@Override
	public Fields getOutputFields()
        {
            return new Fields(FIELD_PLACE, FIELD_DAY, FIELD_LATITUDE, FIELD_LONGITUDE, FIELD_SUMMARY, FIELD_PRECIPPROBABILITY,
            		FIELD_PRECIPINTENSITY, FIELD_TEMPERATUREMAX, FIELD_TEMPERATUREMIN,
            		FIELD_ICON, FIELD_CLOUDCOVER, FIELD_OZONE, FIELD_HUMIDITY,
            		FIELD_WINDSPEED, FIELD_MOONPHASE, FIELD_WINDBEARING, FIELD_PRESSURE, FIELD_TIME);
		
	}
        
        private String cleanup(String str)
        {
            if (str != null)
            {
                return str.trim().replace("\n", "").replace("\t", "").replace(":", "");
            } 
            else
            {
                return str;
            }
            
        }
}