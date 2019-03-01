package udacity.storm;

/*
 * a class for holding all topology component names
 */
public class TopologyComponents {
    // Spouts
    public static final String MyLikesSpout = "spout-likes";
    public static final String MyNamesSpout = "spout-names";

    // Bolts
    public static final String ExclamationBolt = "bolt-exclamation";
    public static final String ExclamationBolt2 = "bolt-exclamation2";
    public static final String ReportBolt = "bolt-report";

    // Topology
    public static final String Topology = "topology-stream-joins";
}