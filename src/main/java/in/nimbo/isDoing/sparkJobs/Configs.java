package in.nimbo.isDoing.sparkJobs;

import java.io.Serializable;

public interface Configs extends Serializable {

    String get(String key);

    String get(String key, String value);
}
