package consumers;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import org.apache.log4j.Logger;


public class LogWeatherEventsBolt extends BaseRichBolt
{
    private static final Logger LOG = Logger.getLogger(LogWeatherEventsBolt.class);
    
    public void declareOutputFields(OutputFieldsDeclarer ofd) 
    {
       //none prints to the Logger.
    }

    public void prepare(Map map, TopologyContext tc, OutputCollector oc) 
    {
       //no output.
    }

    public void execute(Tuple tuple) 
    {
    	System.out.println(tuple.getStringByField(WeatherScheme.FIELD_DAY));
    	
      LOG.info(tuple.getStringByField(WeatherScheme.FIELD_PLACE)  + "," + 
    		  tuple.getStringByField(WeatherScheme.FIELD_DAY)  + "," + 
    		  tuple.getStringByField(WeatherScheme.FIELD_LATITUDE)  + "," + 
    		  tuple.getStringByField(WeatherScheme.FIELD_LONGITUDE)  + "," + 
              tuple.getStringByField(WeatherScheme.FIELD_SUMMARY)    + "," +
              tuple.getValueByField(WeatherScheme.FIELD_PRECIPPROBABILITY)  + "," +
              tuple.getStringByField(WeatherScheme.FIELD_PRECIPINTENSITY)  + "," +
              tuple.getStringByField(WeatherScheme.FIELD_TEMPERATUREMAX)    + "," +
              tuple.getStringByField(WeatherScheme.FIELD_TEMPERATUREMIN)    + "," +
              tuple.getStringByField(WeatherScheme.FIELD_ICON)    + "," +
              tuple.getStringByField(WeatherScheme.FIELD_CLOUDCOVER)    + "," +
              tuple.getStringByField(WeatherScheme.FIELD_OZONE)    + "," +
              tuple.getStringByField(WeatherScheme.FIELD_HUMIDITY)    + "," +
              tuple.getStringByField(WeatherScheme.FIELD_WINDSPEED)    + "," +
              tuple.getStringByField(WeatherScheme.FIELD_MOONPHASE)    + "," +
              tuple.getStringByField(WeatherScheme.FIELD_WINDBEARING)    + "," +
              tuple.getStringByField(WeatherScheme.FIELD_PRESSURE)    + "," +
              tuple.getStringByField(WeatherScheme.FIELD_TIME));
    }
    
}
