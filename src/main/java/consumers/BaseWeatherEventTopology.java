package consumers;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public abstract class BaseWeatherEventTopology {

	
	private static final Logger LOG = Logger.getLogger(BaseWeatherEventTopology.class);

	protected Properties topologyConfig;
	
	public BaseWeatherEventTopology(String configFileLocation) throws Exception {
		
		topologyConfig = new Properties();
		try {
			System.out.println("Reading config from " + configFileLocation);
			topologyConfig.load(ClassLoader.getSystemResourceAsStream(configFileLocation));
			//topologyConfig.load(configFileLocation);
		    System.out.println("Done reading config from " + configFileLocation);
		} catch (FileNotFoundException e) {
			LOG.error("Encountered error while reading configuration properties: "
					+ e.getMessage());
			throw e;
		} catch (IOException e) {
			LOG.error("Encountered error while reading configuration properties: "
					+ e.getMessage());
			throw e;
		}			
	}
}
