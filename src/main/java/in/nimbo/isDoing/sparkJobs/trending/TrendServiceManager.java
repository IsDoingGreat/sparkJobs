package in.nimbo.isDoing.sparkJobs.trending;

import in.nimbo.isDoing.sparkJobs.Configs;
import in.nimbo.isDoing.sparkJobs.Service;
import in.nimbo.isDoing.sparkJobs.ranking.BackLinks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TrendServiceManager {
    private static final Logger logger = LoggerFactory.getLogger(TrendServiceManager.class.getSimpleName());

    private Configs configs;
    private Map<String, Service> services;

    public TrendServiceManager(Configs configs) {
        logger.info("Starting TrendServiceManager");
        this.configs = configs;
        this.services = new ConcurrentHashMap<>();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> stopAll()));

        logger.info("TrendServiceManager Started");
    }

    public void startService(String name) {
        try {
            if (services.get(name) != null) {
                logger.info(name + " service already running");
            } else {
                switch (name) {
                    case "TwitterTrend":
                        startService(new TwitterTrend(configs));
                        break;

                    case "NewsTrend":
                        startService(new NewsTrend(configs));
                        break;

                    case "BackLinks":
                        startService(new BackLinks(configs));
                        break;

                    default:
                        logger.warn(name + " service Not Found");
                        break;
                }
            }
        } catch (Exception e) {
            logger.error("Error in Creating Service", e);
        }
    }

    public void startService(Service service) throws Exception {
        try {
            if (services.get(service.getName()) == null) {
                service.start();
                services.put(service.getName(), service);
            } else {
                logger.error(service.getName() + " service already running");
            }
        } catch (Exception e) {
            logger.error("Error during starting service.", e);
            logger.error("Stopping " + service.getName() + " service");
            service.stop();
        }
    }


    public void stopService(String serviceName) {
        if (services.get(serviceName) == null) {
            logger.error(serviceName + " service is not running");
            return;
        }

        try {
            services.get(serviceName).stop();
            services.remove(services.get(serviceName));
        } catch (Exception e) {
            logger.error("Error During Stopping Service.", e);
        }
    }

    public Service getService(String name) {
        if (services.containsKey(name)) {
            return services.get(name);
        } else {
            throw new RuntimeException("Service Not Found");
        }
    }

    public void stopAll() {
        try {
            for (Map.Entry<String, Service> entry : services.entrySet()) {
                stopService(entry.getKey());
            }
        } catch (Exception e) {
            logger.error("Error Stopping Service", e);
        }
    }


    public Configs getConfigs() {
        if (configs == null) {
            logger.error("Configs is null");
            throw new IllegalStateException("Config is null");
        }
        return configs;
    }

}
