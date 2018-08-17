package in.nimbo.isDoing.sparkJobs.trending;

import in.nimbo.isDoing.sparkJobs.Configs;
import in.nimbo.isDoing.sparkJobs.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;


public class Trend implements Service {
    static final Logger logger = LoggerFactory.getLogger(Trend.class.getSimpleName());
    static final String SPACE = "\\W";

    JavaStreamingContext javaStreamingContext;

    Set<String> topicsSet;
    Map<String, Object> kafkaParams;

    Configs configs;

    Configuration configuration;
    String path;
    String hBaseTableName;
    String hBaseColumnFamily;
    String quantifier;
    int stopWordLength;
    int trendNumber;

    public Trend(Configs configs) {
        this.configs = configs;
        setUpConfig();
    }

    void setUpConfig() {

    }

    public void start() throws Exception {
        logger.info("Start Trend Spark Task");

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        // Get the lines, split them into words, count the words and print
        // Removing stop words
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.stream(x.split(SPACE)).filter(s -> s.length() > 0).iterator());

        // Calculate count of each word
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        // Sort words to find trending words
        JavaPairDStream<Integer, String> swappedPair = wordCounts.mapToPair(Tuple2::swap);
        JavaPairDStream<Integer, String> sortedWords = swappedPair.transformToPair(
                (Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>) jPairRDD -> jPairRDD.sortByKey(false));

        // Put trending words to HBase table
        sortedWords.foreachRDD(
                rdd -> {

                    HBaseAdmin.checkHBaseAvailable(configuration);
                    Connection connection = ConnectionFactory.createConnection(configuration);

                    rdd.take(trendNumber).forEach(wordCount -> {
                        TableName tn = TableName.valueOf(hBaseTableName);
                        try {
                            Table table = connection.getTable(tn);
                            Put put = new Put(Bytes.toBytes(wordCount._2));
                            put.addColumn(Bytes.toBytes(hBaseColumnFamily), Bytes.toBytes(quantifier), Bytes.toBytes(wordCount._1));
                            table.put(put);
                            table.close();
                        } catch (IOException e) {
                            logger.error("Problem when putting trending result to HBase", e);
                            throw new IllegalStateException("Problem when putting trending result to HBase");
                        }
                    });
                }
        );

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

    @Override
    public void status() {

    }

    @Override
    public void stop() {
        logger.info("Stopping Trend App");
        javaStreamingContext.close();
    }

    @Override
    public String getName() {
        return "Trend";
    }
}