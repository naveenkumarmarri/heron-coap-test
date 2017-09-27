package tutorial;

import com.twitter.heron.common.basics.ByteAmount;
import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import tutorial.util.HelperRunner;

/**
 * This is driver as well the topology graph generator
 */
public class WordCountTopology {

    private WordCountTopology() { }

    //Entry point for the topology
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        //Add the spout, with a name of 'sentence'
        //and parallelism hint of 3 executors
        builder.setSpout("sentence", new RandomSentenceSpout(),3);

        //Add the SplitSentence bolt, with a name of 'split'
        //and parallelism hint of 8 executors
        //shufflegrouping subscribes to the spout, and equally distributes
        //tuples (sentences) across instances of the SplitSentence bolt
        builder.setBolt("split", new SplitSentenceBolt(),8).shuffleGrouping("sentence");

        //Add the counter, with a name of 'count'
        //and parallelis    m hint of 12 executors
        //fieldsGrouping subscribes to the split bolt, and
        //ensures that the same word is sent to the same instance (group by field 'word')
        builder.setBolt("count", new WordCountBolt(),12).fieldsGrouping("split", new Fields("word"));
        Config conf = new Config();

        // Resource Configs
        com.twitter.heron.api.Config.setComponentRam(conf, "sentence", ByteAmount.fromGigabytes(1));
        com.twitter.heron.api.Config.setComponentRam(conf, "split", ByteAmount.fromGigabytes(1));
        com.twitter.heron.api.Config.setComponentRam(conf, "count", ByteAmount.fromGigabytes(1));
        com.twitter.heron.api.Config.setContainerCpuRequested(conf, 3);

        //submit the topology
        HelperRunner.runTopology(args, builder.createTopology(), conf);

    }

}
