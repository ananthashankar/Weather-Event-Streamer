package producer;

/**
 * WeatherEventsProducer class simulates the real time weather event generation.
 *
 */
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;


public class WeatherEventsProducer {

    private static final Logger LOG = Logger.getLogger(WeatherEventsProducer.class);
    private static final String apikey = "132c5a540bc50419b14b334ced3d4a6e";
    private static final HashMap<String, String[]> zones = new HashMap<String, String[]>();
    
    
    public static void main(String[] args) 
            throws ParserConfigurationException, SAXException, IOException, URISyntaxException 
    {
    	
        if (args.length != 2) 
        {
            
            System.out.println("Usage: WeatherEventsProducer <broker list> <zookeeper>");
            System.exit(-1);
        }
        
        zones.put("Las Vegas", new String[]{"36.1699", "115.1398"});
        zones.put("Mexico City", new String[]{"23.6345", "102.5528"});
        zones.put("Toronto", new String[]{"43.6532", "79.3832"});
        zones.put("New York City", new String[]{"40.7128", "74.0059"});
        zones.put("Karachi", new String[]{"24.8615", "67.0099"});
        zones.put("New Delhi", new String[]{"28.6139", "77.2090"});
        zones.put("Manila", new String[]{"14.5995", "120.9842"});
        zones.put("Sydney", new String[]{"33.8675", "151.2070"});
        zones.put("Seoul", new String[]{"37.5665", "126.9780"}); 
        zones.put("Tokyo", new String[]{"35.6895", "139.6917"});
        zones.put("Cairo", new String[]{"30.0444", "31.2357"});
        zones.put("Los Angeles", new String[]{"34.0522", "118.2437"});
        zones.put("Rio de Janeiro", new String[]{"22.9068", "43.1729"});
        zones.put("Washington DC", new String[]{"38.9072", "77.0369"});
        zones.put("Beijing", new String[]{"39.9042", "116.4074"});
        zones.put("Chicago", new String[]{"41.8781", "87.6298"}); 
        zones.put("London", new String[]{"51.5074", "0.1278"});
        zones.put("Paris", new String[]{"48.8566", "2.3522"}); 
        zones.put("Madrid", new String[]{"40.4168", "3.7038"}); 
        zones.put("Boston", new String[]{"42.3601", "71.0589"});
        
        LOG.debug("Using broker list:" + args[0] +", zk conn:" + args[1]);
        
        // long events = Long.parseLong(args[0]);
        Random rnd = new Random();
        
        Properties props = new Properties();
        props.put("metadata.broker.list", "sandbox.hortonworks.com:6667");
        props.put("zk.connect", "sandbox.hortonworks.com:2181");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        String TOPIC = "weather-topic";
        ProducerConfig config = new ProducerConfig(props);
        
        int k = 1;
        
        
        while(k < 50) 
        {
        	Producer<String, String> producer = new Producer<String, String>(config);
        	for(String key : zones.keySet()){
        	
		    	ForecastIO fio = new ForecastIO(apikey);
				fio.setUnits(ForecastIO.UNITS_SI);
				fio.setLang(ForecastIO.LANG_ENGLISH);
				// fetches only daily data
				fio.setExcludeURL("hourly,minutely");
				fio.getForecast(zones.get(key)[0], zones.get(key)[1]);
				
				//Daily data
				FIODaily daily = new FIODaily(fio);
				if(daily.days()<0){
					System.out.println("No daily data.");
				} else {
					try {
						for(int i = 0; i<daily.days(); i++){
							
							String messageSent = key + "\n";
							String [] h = daily.getDay(i).getFieldsArray();
							messageSent += "Day #"+(i+1) +"\n";
							messageSent += "latitude:" + zones.get(key)[0] + "\n";
							messageSent += "longitude:" + zones.get(key)[1] + "\n";
							
							for(int j=0; j<h.length; j++){
								messageSent += h[j]+": "+daily.getDay(i).getByKey(h[j]) + "\n";
							}
						
			                KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, messageSent);
			                producer.send(data);
						}
			                producer.close();
			                k++;
			                Thread.sleep(10000);
		            } catch (Exception e) {
		                e.printStackTrace();
		            }
				}
        }
        }
        
    }

}
