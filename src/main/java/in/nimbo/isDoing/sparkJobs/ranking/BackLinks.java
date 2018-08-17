package in.nimbo.isDoing.sparkJobs.ranking;

import in.nimbo.isDoing.sparkJobs.Configs;
import in.nimbo.isDoing.sparkJobs.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

public class BackLinks implements Service {
    private static final Logger logger = LoggerFactory.getLogger(BackLinks.class.getSimpleName());

    private JavaSparkContext javaSparkContext;
    private Configuration configuration;
    private String path;

    private String hBaseInputTableName;
    private String hBaseInputColumnFamily;
    private String inputQuantifier;

    private String hBaseOutputTableName;
    private  String hBaseOutputColumnFamily;
    private String outputQuantifier;

    public BackLinks(Configs configs) {
        logger.info("Setup Spark Configuration");
        String master = configs.get("rending.backLinks.spark.master");
        SparkConf sparkConf = new SparkConf().setAppName(BackLinks.class.getSimpleName()).setMaster(master);
        javaSparkContext = new JavaSparkContext(sparkConf);


        logger.info("Setup HBase Configuration");
        configuration = HBaseConfiguration.create();
        path = BackLinks.class.getClassLoader().getResource(configs.get("trending.hbase.site")).getPath();
        configuration.addResource(new Path(path));

        hBaseInputTableName = configs.get("trending.backLinks.hbase.backLinks.tableName");
        hBaseInputColumnFamily = configs.get("trending.backLinks.hbase.backLinks.columnFamily");
        inputQuantifier = configs.get("trending.backLinks.hbase.backLinks.quantifier");

        hBaseOutputTableName = configs.get("trending.backLinks.hbase.references.tableName");
        hBaseOutputColumnFamily = configs.get("trending.backLinks.hbase.references.columnFamily");
        outputQuantifier = configs.get("trending.backLinks.hbase.references.quantifier");

        configuration.set(TableInputFormat.INPUT_TABLE, hBaseInputTableName);
        configuration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hBaseInputColumnFamily);
    }

    public void start() {
        logger.info("Start BackLinks Spark Task");
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseData =
                javaSparkContext.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<String, String> pairRDD = hBaseData.flatMapToPair(record -> {
            String key = Bytes.toString(record._1.get());
            List<Cell> linkCells = record._2.listCells();
            return linkCells.stream().map(cell -> new Tuple2<>(Bytes.toString(CellUtil.cloneValue(cell)), key)).iterator();
        });

        JavaPairRDD<String, Integer> mapToOne = pairRDD.mapToPair(r -> new Tuple2<>(r._1, 1));
        JavaPairRDD<String, Integer> mapToRefCount = mapToOne.reduceByKey((v1, v2) -> v1 + v2);

        Job job = null;
        try {
            job = Job.getInstance(configuration);
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hBaseOutputTableName);
            job.setOutputFormatClass(TableOutputFormat.class);
        } catch (IOException e) {
            logger.error("Problem when creating Map/Reduce Job", e);
            throw new IllegalStateException("Problem when creating Map/Reduce Job");
        }

        JavaPairRDD<ImmutableBytesWritable, Put> hBaseBulkPut = mapToRefCount.mapToPair(
                record -> {
                    String link = record._1;
                    int count = record._2;

                    Put put = new Put(Bytes.toBytes(link));
                    put.addColumn(Bytes.toBytes("refCount"), Bytes.toBytes("count"), Bytes.toBytes(count));

                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                });

        hBaseBulkPut.saveAsNewAPIHadoopDataset(job.getConfiguration());

        javaSparkContext.stop();
    }

    @Override
    public void status() throws Exception {

    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping BackLinks App...");
        javaSparkContext.close();
        configuration.clear();
    }

    @Override
    public String getName() throws Exception {
        return "BackLinks";
    }
}
