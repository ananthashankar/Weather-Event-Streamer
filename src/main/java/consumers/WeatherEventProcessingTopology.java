package consumers;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

/**
 *
 * @author 
 */
public class WeatherEventProcessingTopology extends BaseWeatherEventTopology 
{
    private static final String KAFKA_SPOUT_ID = "kafkaSpout"; 
    private static final String HDFS_BOLT_ID = "hdfsBolt";
    private static final String HBASE_BOLT_ID = "hbaseBolt"; 
    private static final String LOG_WEATHER_BOLT_ID = "logWeatherEventBolt";
            
    public WeatherEventProcessingTopology(String configFileLocation) throws Exception 
    {
        super(configFileLocation);
    }

    private SpoutConfig constructKafkaSpoutConf() 
    {
        BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host.port"));
        String topic = topologyConfig.getProperty("kafka.topic");
        String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
        String consumerGroupId = "StormSpout";

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);

        /* Custom WeatherScheme that will take Kafka message of single weatherEvent 
         * and emit a 2-tuple consisting of weatherId and weatherEvent. This driverId
         * is required to do a fieldsSorting so that all driver events are sent to the set of bolts */
        spoutConfig.scheme = new SchemeAsMultiScheme(new WeatherScheme());

        return spoutConfig;
    }
    
    
    
    public void configureKafkaSpout(TopologyBuilder builder) 
    {
        KafkaSpout kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());
        int spoutCount = Integer.valueOf(topologyConfig.getProperty("spout.thread.count"));
        builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout);
    }
    
    
    public void configureHDFSBolt(TopologyBuilder builder) 
    {
        // Use pipe as record boundary

        String rootPath = topologyConfig.getProperty("hdfs.path");
        String prefix = topologyConfig.getProperty("hdfs.file.prefix");
        String fsUrl = topologyConfig.getProperty("hdfs.url");
        String sourceMetastoreUrl = topologyConfig.getProperty("hive.metastore.url");
        String hiveStagingTableName = topologyConfig.getProperty("hive.staging.table.name");
        String databaseName = topologyConfig.getProperty("hive.database.name");
        Float rotationTimeInMinutes = Float.valueOf(topologyConfig.getProperty("hdfs.file.rotation.time.minutes"));

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(",");

        //Synchronize data buffer with the filesystem every 100 tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(100);

        // Rotate data files when they reach five MB
        //FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

        //Rotate every X minutes
        FileTimeRotationPolicy rotationPolicy = new FileTimeRotationPolicy
                    (rotationTimeInMinutes, FileTimeRotationPolicy.Units.MINUTES);

        //Hive Partition Action
        HiveTablePartitionAction hivePartitionAction = new HiveTablePartitionAction
                    (sourceMetastoreUrl, hiveStagingTableName, databaseName, fsUrl);

        //MoveFileAction moveFileAction = new MoveFileAction().toDestination(rootPath + "/working");



        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                        .withPath(rootPath + "/staging")
                        .withPrefix(prefix);

        // Instantiate the HdfsBolt
        HdfsBolt hdfsBolt = new HdfsBolt()
                         .withFsUrl(fsUrl)
                 .withFileNameFormat(fileNameFormat)
                 .withRecordFormat(format)
                 .withRotationPolicy(rotationPolicy)
                 .withSyncPolicy(syncPolicy)
                 .addRotationAction(hivePartitionAction);

       // int hdfsBoltCount = Integer.valueOf(topologyConfig.getProperty("hdfsbolt.thread.count"));
        builder.setBolt(HDFS_BOLT_ID, hdfsBolt, 2).shuffleGrouping(KAFKA_SPOUT_ID);
    }
    
    public void configureHBaseBolt(TopologyBuilder builder)
    {
        WeatherHBaseBolt hbaseBolt = new WeatherHBaseBolt(topologyConfig);
        builder.setBolt(HBASE_BOLT_ID, hbaseBolt, 2).shuffleGrouping(KAFKA_SPOUT_ID);
    }
    
    public void configureLogWeatherEventBolt(TopologyBuilder builder)
    {
        LogWeatherEventsBolt logBolt = new LogWeatherEventsBolt();
        builder.setBolt(LOG_WEATHER_BOLT_ID, logBolt).globalGrouping(KAFKA_SPOUT_ID);
    }
    
    private void buildAndSubmit() throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();
        configureKafkaSpout(builder);
        //configureLogWeatherEventBolt(builder);
        configureHDFSBolt(builder);
        
        configureHBaseBolt(builder);
        
        //configureOverspeedMonitorBolt(builder);
        
        Config conf = new Config();
	conf.setDebug(true);
        
        StormSubmitter.submitTopology("weather-event-processor", 
                                    conf, builder.createTopology());
    }

    public static void main(String[] str) throws Exception
    {
        String configFileLocation = "weather_event_topology.properties";
        WeatherEventProcessingTopology weatherTopology 
                = new WeatherEventProcessingTopology(configFileLocation);
        weatherTopology.buildAndSubmit();
    }

}
