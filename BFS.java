import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;
import sun.management.counter.LongCounter;

/**
 * Created by xing on 10/07/17.
 */
public class BFS {

    private static int maxIterations;
    private static String outputPath;
    private static String edgesInputPath;
    private static int srcVertexId;
    private static int peer;
    private static int numVertices;

    public static void main(String[] args) throws Exception {
        if (!parseParameters(args)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(peer);

        DataSet<Tuple3<Integer, Integer, NullValue>> edges = env.readTextFile(edgesInputPath).map(
            new MapFunction<String, Tuple3<Integer, Integer, NullValue>>() {
                @Override
                public Tuple3<Integer, Integer, NullValue> map(String s) throws Exception {

                    String[] parts = s.split("\t");
                    if( parts.length != 2){
                        throw new Exception("Input file error");
                    }

                    return new Tuple3<>(Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim()), new NullValue());
                }

            }).partitionByHash(0);

        DataSet<Long> ver = env.generateSequence(0, numVertices - 1);

        DataSet<Tuple2<Integer,Integer>> vertices = ver.map(new LevelAssigner(srcVertexId));

        Graph<Integer, Integer, NullValue> graph = Graph
            .fromTupleDataSet(vertices , edges, env);

        Graph<Integer, Integer, NullValue> result = graph
            .runVertexCentricIteration(new BFSComputeFunction(), new BFSCombiner(), maxIterations);

        DataSet<Vertex<Integer, Integer>> vertexWithBFS = result.getVertices();

        vertexWithBFS.writeAsCsv(outputPath, "\n", ",", WriteMode.OVERWRITE);

        env.execute("Pregel Single Source Shortest Paths Example");
    }

    private static boolean parseParameters(String[] args) {

        if (args.length != 6) {
            System.err.println("Usage: SingleSourceShortestPaths <source vertex id>" +
                " <input edges path> <output path> <num iterations> <peer> <vertex number>");
            return false;
        }
        srcVertexId = Integer.parseInt(args[0]);
        edgesInputPath = args[1];
        outputPath = args[2];
        maxIterations = Integer.parseInt(args[3]);
        peer = Integer.parseInt(args[4]);
        numVertices = Integer.parseInt(args[5]);
        return true;
    }

    private static class BFSComputeFunction extends
        ComputeFunction<Integer, Integer, NullValue, Integer> {

        @Override
        public void compute(Vertex<Integer, Integer> vertex, MessageIterator<Integer> receiveMessageIterator)
            throws Exception {
            if(getSuperstepNumber()==1){
                if(vertex.getValue() == 0){
                    sendMessageToAllNeighbors(1);
                }
                return;
            }
            int val = Integer.MAX_VALUE;
            for (int message : receiveMessageIterator) {
                val = Math.min(val, message);
            }

            if (val < vertex.getValue()) {
                setNewVertexValue(val); //delta set
                sendMessageToAllNeighbors(val + 1);
            }
        }
    }

    public static final class LevelAssigner implements MapFunction<Long, Tuple2<Integer,Integer>> {
        // for we cannot use index to get data serially so we need a manual index
        private int startpoint;
        public LevelAssigner(int inn){
            startpoint = inn;
        }
        @Override
        public Tuple2<Integer,Integer> map(Long vertex){
            //System.out.println(vertex);
            if(vertex.intValue() == (startpoint)){
                //System.out.println(startpoint);
                return new Tuple2<>(startpoint,0);
            } else {
                return new Tuple2<>(vertex.intValue(), Integer.MAX_VALUE);
            }
        }
    }

    private static class BFSCombiner extends MessageCombiner<Integer, Integer> {

        @Override
        public void combineMessages(MessageIterator<Integer> sendMessageIterator) throws Exception {
            int val = Integer.MAX_VALUE;
            for (Integer msg : sendMessageIterator) {
                val = Math.min(val, msg);
            }
            sendCombinedMessage(val);//next workset
        }
    }
}
