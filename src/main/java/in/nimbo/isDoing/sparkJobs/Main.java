package in.nimbo.isDoing.sparkJobs;

import in.nimbo.isDoing.sparkJobs.trending.TrendServiceManager;
import in.nimbo.isDoing.sparkJobs.trending.TrendServiceManagerConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class.getSimpleName());

    public static void main(String[] args) {
        if (args.length < 1) {
            logger.error("Invalid args");
            System.exit(0);
        }
        String appName = args[0];

        Configs trendServiceManagerConfigs = null;
        TrendServiceManager trendServiceManager = null;
        try {
            trendServiceManagerConfigs = new TrendServiceManagerConfigs();
            trendServiceManager = new TrendServiceManager(trendServiceManagerConfigs);
        } catch (Exception e) {
            logger.error("Problem when creating instance of TrendServiceManagerConfigs: ", e);
            throw new IllegalStateException("roblem when creating instance of TrendServiceManagerConfigs");
        }

//        TrendServiceManager trendServiceManager = new TrendServiceManager(trendServiceManagerConfigs);
        trendServiceManager.startService(appName);
    }

}