package consumers;


import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WeatherHBaseBolt implements IRichBolt 
{
    private static final long serialVersionUID = 2946379346389650318L;
    private static final Logger LOG = Logger.getLogger(WeatherHBaseBolt.class);

    //TABLES
    private static final String EVENTS_TABLE_NAME =  "weather_events";

    //CF
    private static final byte[] CF_EVENTS_TABLE = Bytes.toBytes("events");
  
    //COL
    
    public static final byte[] COL_PLACE = Bytes.toBytes("place");
    public static final byte[] COL_DAY = Bytes.toBytes("day");
    public static final byte[] COL_LATITUDE = Bytes.toBytes("latitude");
    public static final byte[] COL_LONGITUDE = Bytes.toBytes("longitude");
	public static final byte[] COL_SUMMARY = Bytes.toBytes("summ");
	public static final byte[] COL_PRECIPPROBABILITY = Bytes.toBytes("precProb");
	public static final byte[] COL_PRECIPINTENSITY = Bytes.toBytes("precInt");
	public static final byte[] COL_TEMPERATUREMAX = Bytes.toBytes("tempMax");
	public static final byte[] COL_TEMPERATUREMIN = Bytes.toBytes("tempMin");
	public static final byte[] COL_ICON = Bytes.toBytes("icon");
	public static final byte[] COL_CLOUDCOVER = Bytes.toBytes("cloudCover");
	public static final byte[] COL_OZONE = Bytes.toBytes("ozone");
	public static final byte[] COL_HUMIDITY = Bytes.toBytes("humidity");
	public static final byte[] COL_WINDSPEED = Bytes.toBytes("windSpeed");
	public static final byte[] COL_MOONPHASE = Bytes.toBytes("moonPhase");
	public static final byte[] COL_WINDBEARING = Bytes.toBytes("windBearing");
	public static final byte[] COL_PRESSURE = Bytes.toBytes("pressure");
	public static final byte[] COL_ALERTS = Bytes.toBytes("alerts");
    
    private static final byte[] COL_COUNT_VALUE = Bytes.toBytes("value");


    private OutputCollector collector;
    private HConnection connection;
    private HTableInterface eventsCountTable;
    private HTableInterface eventsTable;

    public WeatherHBaseBolt(Properties topologyConfig) 
    {
    	
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) 
    {
        this.collector = collector;
        try 
        {
            this.connection = HConnectionManager.createConnection(constructConfiguration());
            this.eventsTable = connection.getTable(EVENTS_TABLE_NAME);
        } 
        catch (Exception e) 
        {
            String errMsg = "Error retrievinging connection and access to HBase Tables";
            LOG.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }		
    }

    @Override
    public void execute(Tuple tuple)
    {

        //LOG.info("About to insert tuple["+input +"] into HBase...");
    	String place = tuple.getStringByField(WeatherScheme.FIELD_PLACE);
        String day = tuple.getStringByField(WeatherScheme.FIELD_DAY);
        String latitude = tuple.getStringByField(WeatherScheme.FIELD_LATITUDE);
        String longitude = tuple.getStringByField(WeatherScheme.FIELD_LONGITUDE);
        String summary = tuple.getStringByField(WeatherScheme.FIELD_SUMMARY);
        String precipProbability = tuple.getStringByField(WeatherScheme.FIELD_PRECIPPROBABILITY);
        String precipIntensity = tuple.getStringByField(WeatherScheme.FIELD_PRECIPINTENSITY);
        String temperatureMax = tuple.getStringByField(WeatherScheme.FIELD_TEMPERATUREMAX);
        String temperatureMin = tuple.getStringByField(WeatherScheme.FIELD_TEMPERATUREMIN);
        String icon = tuple.getStringByField(WeatherScheme.FIELD_ICON);
        String cloudCover = tuple.getStringByField(WeatherScheme.FIELD_CLOUDCOVER);
        String ozone = tuple.getStringByField(WeatherScheme.FIELD_OZONE);
        String humidity = tuple.getStringByField(WeatherScheme.FIELD_HUMIDITY);
        String windSpeed = tuple.getStringByField(WeatherScheme.FIELD_WINDSPEED);
        String moonPhase = tuple.getStringByField(WeatherScheme.FIELD_MOONPHASE);
        String windBearing = tuple.getStringByField(WeatherScheme.FIELD_WINDBEARING);
        String pressure = tuple.getStringByField(WeatherScheme.FIELD_PRESSURE);
        String alerts = tuple.getStringByField(WeatherScheme.FIELD_ALERTS);

      //  long incidentTotalCount = getInfractionCountForDriver(driverId);

        try 
        {

            Put put = constructRow(EVENTS_TABLE_NAME, place, day, latitude, longitude, summary, precipProbability, precipIntensity,
            		temperatureMax, temperatureMin, icon, cloudCover, ozone, humidity, windSpeed, 
            		moonPhase, windBearing, pressure, alerts );
            this.eventsTable.put(put);

        } 
        catch (Exception e) 
        {
            LOG.error("Error inserting event into HBase table["+EVENTS_TABLE_NAME+"]", e);
        }

        collector.emit(tuple, new Values(place, day, latitude, longitude, summary, precipProbability, precipIntensity,
        		temperatureMax, temperatureMin, icon, cloudCover, ozone, humidity, windSpeed, 
        		moonPhase, windBearing, pressure, alerts));
        //acknowledge even if there is an error
        collector.ack(tuple);
    }


    public static  Configuration constructConfiguration()
    {
        Configuration config = HBaseConfiguration.create();
        return config;
    }	


    private Put constructRow(String columnFamily, String place, String day, String latitude, 
    		String longitude, String summary, String precipProbability,
    		String precipIntensity, String temperatureMax, String temperatureMin, String icon,
    		String cloudCover, String ozone, String humidity, String windSpeed, 
    		String moonPhase, String windBearing, String pressure, String alerts) 
    {

        String rowKey = consructKey(day, place);
        Put put = new Put(Bytes.toBytes(rowKey));
        
        put.add(CF_EVENTS_TABLE, COL_PLACE, Bytes.toBytes(place));
        put.add(CF_EVENTS_TABLE, COL_DAY, Bytes.toBytes(day));
        put.add(CF_EVENTS_TABLE, COL_LATITUDE, Bytes.toBytes(latitude));
        put.add(CF_EVENTS_TABLE, COL_LONGITUDE, Bytes.toBytes(longitude));
        put.add(CF_EVENTS_TABLE, COL_SUMMARY, Bytes.toBytes(summary));
        put.add(CF_EVENTS_TABLE, COL_PRECIPPROBABILITY, Bytes.toBytes(precipProbability));
        put.add(CF_EVENTS_TABLE, COL_PRECIPINTENSITY, Bytes.toBytes(precipIntensity));
        put.add(CF_EVENTS_TABLE, COL_TEMPERATUREMAX, Bytes.toBytes(temperatureMax));
        put.add(CF_EVENTS_TABLE, COL_TEMPERATUREMIN, Bytes.toBytes(temperatureMin));
        put.add(CF_EVENTS_TABLE, COL_ICON, Bytes.toBytes(icon));
        put.add(CF_EVENTS_TABLE, COL_CLOUDCOVER, Bytes.toBytes(cloudCover));
        put.add(CF_EVENTS_TABLE, COL_OZONE, Bytes.toBytes(ozone));
        put.add(CF_EVENTS_TABLE, COL_HUMIDITY, Bytes.toBytes(humidity));
        put.add(CF_EVENTS_TABLE, COL_WINDSPEED, Bytes.toBytes(windSpeed));
        put.add(CF_EVENTS_TABLE, COL_MOONPHASE, Bytes.toBytes(moonPhase));
        put.add(CF_EVENTS_TABLE, COL_WINDBEARING, Bytes.toBytes(windBearing));
        put.add(CF_EVENTS_TABLE, COL_PRESSURE, Bytes.toBytes(pressure));
        put.add(CF_EVENTS_TABLE, COL_ALERTS, Bytes.toBytes(alerts));
        

        return put;
    }


    private String consructKey(String day, String place)
    {
       
        String rowKey = day+"|"+place;
        return rowKey;
    }	


    @Override
    public void cleanup() 
    {
        try 
        {
                eventsTable.close();
                connection.close();
        } 
        catch (Exception  e) 
        {
                LOG.error("Error closing connections", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(WeatherScheme.FIELD_PLACE, WeatherScheme.FIELD_DAY, 
            		WeatherScheme.FIELD_LATITUDE, WeatherScheme.FIELD_LONGITUDE, WeatherScheme.FIELD_SUMMARY,
            		WeatherScheme.FIELD_PRECIPPROBABILITY, WeatherScheme.FIELD_PRECIPINTENSITY, 
            		WeatherScheme.FIELD_TEMPERATUREMAX, WeatherScheme.FIELD_TEMPERATUREMIN, 
            		WeatherScheme.FIELD_ICON, WeatherScheme.FIELD_CLOUDCOVER, WeatherScheme.FIELD_OZONE,
            		WeatherScheme.FIELD_HUMIDITY, WeatherScheme.FIELD_WINDSPEED, WeatherScheme.FIELD_MOONPHASE,
            		WeatherScheme.FIELD_WINDBEARING, WeatherScheme.FIELD_PRESSURE, WeatherScheme.FIELD_ALERTS));
    }

    @Override
    public Map<String, Object> getComponentConfiguration()
    {
            return null;
    }

    private long getInfractionCountForDriver(String driverId)
    {

        try 
        {
            byte[] driver = Bytes.toBytes(driverId);
            Get get = new Get(driver);
            Result result = eventsCountTable.get(get);
            long count = 0;
            return count;
        } 
        catch (Exception e) 
        {
            LOG.error("Error getting infraction count", e);
            throw new RuntimeException("Error getting infraction count");
        }
    }	
}
