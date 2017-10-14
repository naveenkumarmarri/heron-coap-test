package tutorial;

import backtype.storm.metric.api.GlobalMetrics;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import org.eclipse.californium.core.CoapServer;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;
import org.eclipse.californium.core.server.resources.CoapExchange;
import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.network.CoapEndpoint;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This is a Sample topology for storm where we consume a stream of words and executes an append operation to them.
 */
public final class ExclamationTopology_Simulator{

    private ExclamationTopology_Simulator() {}

    public static void main(String[] args) throws Exception {

        // Instantiate a topology builder to build the tag
        TopologyBuilder builder = new TopologyBuilder();


        // Define the parallelism hint for the topololgy
        final int parallelism = 1;

        // Build the topology to have a 'word' spout and 'exclaim' bolt
        // also, set the 'word' spout bolt to have two instances
        builder.setSpout("word", new TestWordSpout(), parallelism);

        // Specify that 'exclaim1' bolt should consume from 'word' spout using
        // Shuffle grouping running in four instances
        builder.setBolt("exclaim", new ExclamationBolt(), 2 * parallelism)
                .shuffleGrouping("word");

        // Create the config for the topology
        Config conf = new Config();

        // Set the run mode to be debug
        conf.setDebug(true);
        
        //conf.setSkipMissingKryoRegistrations(false);



        // Set the number of tuples to be in flight at any given time to be 10
        conf.setMaxSpoutPending(10);
        conf.setMessageTimeoutSecs(600);

        // Set JVM options to dump the heap when out of memory
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
        //conf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, new CoapServer(8888));

        // If the topology name is specified run in the cluster mode, otherwise run in
        // Simulator mode
        if (args != null && args.length > 0) {

            // Set the number of containers to be two
            conf.setNumWorkers(parallelism);

            // Submit the topology to be run in a cluster
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            System.out.println("Topology name not provided as an argument, running in simulator mode.");

            // Create the local cluster for simulation
            LocalCluster cluster = new LocalCluster();

            // Submit the topology to the simulated cluster
            cluster.submitTopology("test", conf, builder.createTopology());

            // Wait for it run 10 secs
            Utils.sleep(10000);

            // Kill and shutdown the topology after the elapsed time
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }


    /**
     * Word Spout that outputs a ranom word among a list of words continuously
     */
    static class TestWordSpout extends BaseRichSpout implements Serializable{

        private static final long serialVersionUID = -3217886193225455451L;
        private SpoutOutputCollector collector;
        private String URI;
        private CoapServer server;
        private String content;
     

        // Intantiate with no throttle duration
        public TestWordSpout()
        {
        	//this.server = server;
        	
        	
        }
        
            
        // Intantiate with specified throtte duration
        
        
        @SuppressWarnings("rawtypes")
        public void open(
                Map conf,
                TopologyContext context,
                SpoutOutputCollector collector) {
        	
        	
        	
        	ExecutorService executorService = Executors.newFixedThreadPool(1);

        	executorService.execute(new Runnable() {
        	    public void run() {
        	    	server = new CoapServer(1112);
                	//server.addEndpoint(new CoapEndpoint(5683));
                	server.add(new CoapResource("helloWorld")  {
        	        		 @Override
        	        	 	   public void handlePUT(CoapExchange exchange) {
        	        	 		 content = exchange.getRequestText();
        	        	 		 exchange.respond(ResponseCode.CHANGED);
        	        	 		 
        	        	 		 }
                	    }); 
                	server.start();
                	System.out.println("after server started");
        	    }
        	});

        	
        	
        	//server.addEndpoint(new EndPoint("helloWorld"));
        	this.collector = collector;	
        	
        	System.out.println("after server started");
			
        }
        
       
        
        // This method is called to generate the next sequence for the spout
        public void nextTuple() {

         collector.emit(new Values(content));
        	
        }


        // This method speisifes the output field labels
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word")); // Here we tagethe output tuple with the tag "word"
        }

	

    }
    


    /**
     * ExclamationBolt Bolt that takes a string word as outputs the same word with exclamation marks appended
     */
    
    
    static class ExclamationBolt extends BaseRichBolt {

        private static final long serialVersionUID = 1184860508880121352L;
        private long nItems;
        private long startTime;
        private OutputCollector collector;

        @Override
        @SuppressWarnings("rawtypes")
        public void prepare(Map conf,
                            TopologyContext context,
                            OutputCollector collector) {
            this.nItems = 0;
            this.collector = collector;
            this.startTime = System.currentTimeMillis();
        }

        @Override
        public void execute(Tuple tuple) {
            // We execute on only every 100000 instance of reading from the "word" spout for this example
            if (++nItems % 100000 == 0) {
                long latency = System.currentTimeMillis() - startTime;
                // Generate the appended word , emit it as output and log it in the console
                final String appendedWord = tuple.getString(0) + "!!!";
                this.collector.emit(new Values(appendedWord));
                System.out.println(appendedWord);
                // Print out and record basic latency measurements in the conolse and metric registry
                System.out.println("Bolt processed " + nItems + " tuples in " + latency + " ms");
                GlobalMetrics.incr("selected_items");
            }
        }

        // This method speisifes the output field labels
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("modified_word")); // Here we tagethe output tuple with the tag "modified_word"
        }
    }
}
