package in.nimbo.isDoing.sparkJobs.trending;

import in.nimbo.isDoing.sparkJobs.Configs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class TrendServiceManagerConfigs implements Configs {

    private static final Logger logger = LoggerFactory.getLogger(TrendServiceManagerConfigs.class);
    private Properties configs = new Properties();

    public TrendServiceManagerConfigs() throws IOException {
        Path configPath = Paths.get("./configs.properties");
        configs.load(new FileInputStream(configPath.toFile()));
        logger.info("Config file loaded", configs);
    }

    @Override
    public String get(String key) {
        String property = configs.getProperty(key);
        if (property != null)
            return property;

        throw new RuntimeException("Config {" + key + "} Not Found");
    }

    @Override
    public String get(String key, String value) {
        return configs.getProperty(key, value);
    }
}
