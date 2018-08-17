package in.nimbo.isDoing.sparkJobs;

public interface Service {
    void start() throws Exception;

    void status() throws Exception;

    void stop() throws Exception;

    String getName() throws Exception;
}