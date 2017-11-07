import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.types.NullValue;
import org.apache.flink.graph.Graph;

/**
 * Created by sanquan.qz on 2017/9/19.
 */
public class GASBfs {

    private static int peer;
    private static String edgesInputPath;
    private static int srcVerId;
    private static int maxIteration;
    private static int numVertices;

    public static void main(String[] args) throws Exception{
        ParameterTool paras = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        peer = paras.getInt("para");

        srcVerId = paras.getInt("start", 1);

        edgesInputPath = paras.get("file");

        maxIteration = paras.getInt("iterations",10);

        numVertices = paras.getInt("vertex");

        env.setParallelism(peer);

        DataSet<Tuple3<Integer, Integer, NullValue>> edges = env.readTextFile(edgesInputPath).map(
            new MapFunction<String, Tuple3<Integer, Integer,NullValue>>() {
                @Override
                public Tuple3<Integer, Integer,NullValue> map(String value) throws Exception {
                    String[] elem = value.split("\t");
                    if(elem.length != 2){
                        throw new Exception("Input format error");
                    }
                    int u = Integer.parseInt(StringUtils.trim(elem[0]));
                    int v = Integer.parseInt(StringUtils.trim(elem[1]));
                    return new Tuple3<>(u,v,new NullValue());
                }
            }
        ).partitionByHash(0);

        DataSet<Long> ver = env.generateSequence(0, numVertices - 1);

        DataSet<Tuple2<Integer,Integer>> vertices = ver.map(new LevelAssigner(srcVerId));

        Graph<Integer, Integer, NullValue> g = Graph.fromTupleDataSet(vertices ,edges , env);

        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
        // set the iteration name
        parameters.setName("GAS Iteration");
        // set the parallelism
        parameters.setParallelism(peer);

        Graph<Integer,Integer,NullValue> res =  g.runScatterGatherIteration(
            new ScatterFunction<Integer, Integer, Integer, NullValue>() {
                public void sendMessages(Vertex<Integer,Integer> vertex){
                    //System.out.println(vertex.getId());
                    if(vertex.getValue() != Integer.MAX_VALUE) {
                        for (Edge<Integer, NullValue> e : getEdges()) {
                            sendMessageTo(e.getTarget(), vertex.getValue() + 1);
                        }
                    }
                }
            },
            new GatherFunction<Integer, Integer, Integer>() {
                public void updateVertex(Vertex<Integer,Integer> vertex, MessageIterator<Integer> inMessages){
                    int minDis = Integer.MAX_VALUE;
                    for (int x : inMessages){
                        if(x < minDis && x > 0){
                            minDis = x;
                        }
                    }
                    if(vertex.getValue() > minDis){
                        setNewVertexValue(minDis);
                    }
                }
            },
            maxIteration,
            parameters
        );

        DataSet<Vertex<Integer,Integer>> bfsres = res.getVertices();

        if(paras.has("output")){
            bfsres.writeAsCsv(paras.get("output"),System.lineSeparator()," ", WriteMode.OVERWRITE);
            env.execute("GAS ITER");
        } else{
            bfsres.print();
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
}
