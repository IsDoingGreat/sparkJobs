package in.nimbo.isDoing.sparkJobs.trending;

import in.nimbo.isDoing.sparkJobs.Configs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class TwitterTrend extends Trend {

    private static final Logger logger = LoggerFactory.getLogger(TwitterTrend.class.getSimpleName());

    public TwitterTrend(Configs configs) {
        super(configs);
    }

    @Override
    void setUpConfig() {
        logger.info("Setup Spark Configuration");
        String master = configs.get("trending.twitterTrend.spark.master");
        stopWordLength = Integer.parseInt(configs.get("trending.twitterTrend.spark.stopWordLength"));
        long durationsSecond = Long.parseLong(configs.get("trending.twitterTrend.spark.durationsSecond"));

        logger.info("Setup Kafka Configuration");
        String brokers = configs.get("trending.twitterTrend.kafka.brokers");
        String groupId = configs.get("trending.twitterTrend.kafka.groupId");
        String topics = configs.get("trending.twitterTrend.kafka.topics");
        boolean enableAutoCommitConfig = Boolean.valueOf(configs.get("trending.twitterTrend.kafka.enableAutoCommitConfig"));
        String autoOffsetResetConfig = configs.get("trending.twitterTrend.kafka.autoOffsetResetConfig");

        SparkConf sparkConf = new SparkConf().setAppName(TwitterTrend.class.getSimpleName()).setMaster(master);
        javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(durationsSecond));

        topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        kafkaParams = new HashMap<>();

        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommitConfig);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);

        logger.info("Setup HBase Configuration");
        configuration = HBaseConfiguration.create();
        path = TwitterTrend.class.getClassLoader().getResource(configs.get("trending.hbase.site")).getPath();
        configuration.addResource(new Path(path));
        hBaseTableName = configs.get("trending.twitterTrend.hbase.twitterTrendWords.tableName");
        hBaseColumnFamily = configs.get("trending.twitterTrend.hbase.twitterTrendWords.columnFamily");
        quantifier = configs.get("trending.twitterTrend.hbase.twitterTrendWords.quantifier");
        trendNumber = Integer.parseInt(configs.get("trending.twitterTrend.hbase.twitterTrendWords.trendNumber"));
    }

    @Override
    public String getName() {
        return "TwitterTrend";
    }
}

