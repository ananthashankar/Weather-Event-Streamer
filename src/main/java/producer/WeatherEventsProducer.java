package producer;

import java.io.BufferedReader;
/**
 * WeatherEventsProducer class simulates the real time weather event generation.
 *
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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

import consumers.SimpleRegressionImpl;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;


public class WeatherEventsProducer {

    private static final Logger LOG = Logger.getLogger(WeatherEventsProducer.class);
    private static final String apikey = "132c5a540bc50419b14b334ced3d4a6e";
    private static final HashMap<String, String[]> zones = new HashMap<String, String[]>();
    //public static HashMap<String, String[]> params = new HashMap<String, String[]>();
    
    
    public static void main(String[] args) 
            throws ParserConfigurationException, SAXException, IOException, URISyntaxException, NumberFormatException, IOException 
    {
    	
        if (args.length != 2) 
        {
            
            System.out.println("Usage: WeatherEventsProducer <broker list> <zookeeper>");
            System.exit(-1);
        }
        
        // prepare model equations
    	SimpleRegressionImpl impl = new SimpleRegressionImpl();
    	
        
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
        		//Producer<String, String> producer = new Producer<String, String>(config);
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
						int dayIn = 0;
						int month = 0;
						
						for(int i = 0; i<daily.days(); i++){
							
							String messageSent = key + "\n";
							String [] h = daily.getDay(i).getFieldsArray();
							messageSent += "Day #"+(i+1) +"\n";
							messageSent += "latitude:" + zones.get(key)[0] + "\n";
							messageSent += "longitude:" + zones.get(key)[1] + "\n";
							
							for(int j=0; j<h.length; j++){
								messageSent += h[j]+": "+daily.getDay(i).getByKey(h[j]).replace(",", "")  + "\n";
								if(h[j].equals("time")){
									dayIn = Integer.parseInt(daily.getDay(i).getByKey(h[j]).replace(",", "").substring(0, 2));
									month = Integer.parseInt(daily.getDay(i).getByKey(h[j]).replace(",", "").substring(3, 5));
								}
							}
							
							if(month == 1){
									 //day of the year
							   } else if(month == 2){
								   dayIn = dayIn + 31; //day of the year
							   } else if(month == 3){
								   dayIn = dayIn + 59; //day of the year
							   } else if(month == 4){
								   dayIn = dayIn + 90; //day of the year
							   } else if(month == 5){
								   dayIn = dayIn + 120; //day of the year
							   } else if(month == 6){
								   dayIn = dayIn + 151; //day of the year
							   } else if(month == 7){
								   dayIn = dayIn + 181; //day of the year
							   } else if(month == 8){
								   dayIn = dayIn + 212; //day of the year
							   } else if(month == 9){
								   dayIn = dayIn + 242; //day of the year
							   } else if(month == 10){
								   dayIn = dayIn + 273; //day of the year
							   } else if(month == 11){
								   dayIn = dayIn + 303; //day of the year
							   } else if(month == 12){
								   dayIn = dayIn + 334; //day of the year
							   }
							
							try {
								FileInputStream fis = new FileInputStream("/opt/WeatherEvents/equation_params");
								BufferedReader in = new BufferedReader(new InputStreamReader(fis));
								String line;
								 while ((line = in.readLine()) != null) {
									 String[] params = line.split(",");
									 if(params[0].equals(key)){
										 
										 String tempMaxModelForecast = String.valueOf(Double.parseDouble(params[1]) + Double.parseDouble(params[4]) * dayIn);
										 String tempMinModelForecast = String.valueOf(Double.parseDouble(params[2]) + Double.parseDouble(params[5]) * dayIn);
										 String pressureModelForecast = String.valueOf(Double.parseDouble(params[3]) + Double.parseDouble(params[6]) * dayIn);
										 messageSent += "tempMaxModelForecast: " + tempMaxModelForecast + "\n";
										 messageSent += "tempMinModelForecast: " + tempMinModelForecast + "\n";
										 messageSent += "pressureModelForecast: " + pressureModelForecast + "\n";
										 
										 break;
									 }
								 }								
								
							} catch(Exception e){
								LOG.error(e);
							}
							
							//Alerts data
							FIOAlerts alerts = new FIOAlerts(fio);
							messageSent += "Alerts:";
							if(alerts.NumberOfAlerts() <= 0){
								messageSent += "No alerts for this location";
							} else {
								System.out.println("Alerts");
								for(int j=0; j<alerts.NumberOfAlerts(); j++)
									messageSent += alerts.getAlert(j).replace("\n", "|").replace(",", "") + " ";
							}
							messageSent += "\n";
			                KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, messageSent);
			                producer.send(data);
						}
			                //producer.close();
			                
			                Thread.sleep(5000);
		            } catch (Exception e) {
		                e.printStackTrace();
		            }
				}
        	}
        	k++;
        	producer.close();
        }
        
    }

}
