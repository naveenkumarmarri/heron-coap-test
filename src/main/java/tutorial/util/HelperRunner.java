package tutorial.util;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.StormTopology;

public class HelperRunner {

    private static final String MODE_OF_OPERATION_CLUSTER = "Cluster";
    private static final String MODE_OF_OPERATION_LOCAL = "Local";
    private static final int NUMBER_OF_WORKERS = 3;  //default value
    private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;

    private static final long MILLIS_IN_SEC = 1000;

    private HelperRunner() {}

    public static void runTopology(String[] args,
                                   StormTopology topology,
                                   Config conf) throws Exception {

        if (args != null) {
            if (args.length < 2) {
                Exception exception = new IllegalArgumentException("Illegal number of command line arguments supplied." +
                        "\nPlease provide the topologyName as the first argument and either " +
                        "'Cluster' or 'Local' as the second argument.");
                throw exception;
            }

            if (!args[1].equals(MODE_OF_OPERATION_CLUSTER) && !args[1].equals(MODE_OF_OPERATION_LOCAL)) {
                Exception exception = new IllegalArgumentException("The allowed values for the second argument is either" +
                        " 'Cluster' or 'Local'. Please provide a valid value for the second argument.");
                throw exception;
            }

            String topologyName = args[0];

            if (args[1].equals(MODE_OF_OPERATION_CLUSTER)) {
                HelperRunner.runTopologyRemotely(topology, topologyName, conf);
            } else {
                conf.setMaxTaskParallelism(NUMBER_OF_WORKERS);
                HelperRunner.runTopologyLocally(topology, topologyName, conf, DEFAULT_RUNTIME_IN_SECONDS);
            }

        }

    }

    private static void runTopologyRemotely(StormTopology topology, String topologyName, Config conf)
            throws InterruptedException, AlreadyAliveException, InvalidTopologyException, NotAliveException {
        StormSubmitter.submitTopology(topologyName, conf, topology);
    }

    private static void runTopologyLocally(StormTopology topology,
                                           String topologyName,
                                           Config conf,
                                           int runtimeInSeconds)
            throws InterruptedException, AlreadyAliveException, InvalidTopologyException, NotAliveException {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);
        Thread.sleep((long) runtimeInSeconds * MILLIS_IN_SEC);
        cluster.killTopology(topologyName);
        cluster.shutdown();
    }
}
