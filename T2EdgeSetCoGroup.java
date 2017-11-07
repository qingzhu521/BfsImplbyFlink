import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

/**
 * Created by sanquan.qz on 2017/7/7.
 */
public class T2EdgeSetCoGroup {

    private static Integer STARTPOINT = 1;

    private static Integer STARTLEVEL = 0;

    private static final Integer MAXPOINT = 2147483647;

    private static String graphfile;
    
    private static Integer nver;
    
    public static void main(String[] args) throws Exception{

        ParameterTool params = ParameterTool.fromArgs(args);

        graphfile = params.get("file");

        nver = params.getInt("vertex");

        STARTPOINT = params.getInt("start", 2);

        final int maxIterations = params.getInt("iterations", 20);

        // get environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(params.getInt("para",200));

        //EdgeInput
        DataSet<Tuple2<Integer, Integer>> edgesInput = env.readTextFile(graphfile).map(
            new MapFunction<String, Tuple2<Integer, Integer>>() {
                @Override
                public Tuple2<Integer, Integer> map(String s) throws Exception {

                    String[] parts = s.split("\t");
                    if (parts.length != 2) {
                        throw new Exception("Input file error");
                    }

                    return new Tuple2<>(Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim()));
                }
            }
        ).partitionByHash(0).sortPartition(0,Order.ASCENDING);

        //Vertex Input
        final Configuration config = new Configuration();
        config.setInteger("start", STARTPOINT);
        DataSet<Tuple2<Integer, Integer>> vertexWithLevel = env.generateSequence(0, nver).map(
            new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
                private int startpt;

                @Override
                public void open(Configuration parameters) throws Exception {
                    startpt = parameters.getInteger("start", 0);
                }

                @Override
                public Tuple2<Integer, Integer> map(Long aLong) throws Exception {
                    if (aLong.intValue() == startpt) {
                        return new Tuple2<>(startpt, 0);
                    } else {
                        return new Tuple2<>(aLong.intValue(), 2147483637);
                    }
                }
            }
        ).withParameters(config).partitionByHash(0);

        DataSet<Tuple2<Integer, Integer>> pointInQ = env.fromElements(new Tuple2<Integer, Integer>(STARTPOINT, STARTLEVEL));

        //Iteration
        DeltaIteration< Tuple2<Integer, Integer>,Tuple2<Integer, Integer> > iterationDelta = vertexWithLevel.iterateDelta(pointInQ, maxIterations,0);

        setUpIteration(iterationDelta, env);

        DataSet<Tuple2<Integer, Integer>> message = iterationDelta.getWorkset().coGroup(edgesInput).where(0).equalTo(0).with(
            new CoGroupFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                @Override
                public void coGroup(Iterable<Tuple2<Integer, Integer>> workset, Iterable<Tuple2<Integer, Integer>> edges,
                                    Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                    Tuple2<Integer, Integer> pt = new Tuple2<>(0, Integer.MAX_VALUE);
                    for (Tuple2<Integer,Integer> i : workset){
                        if(pt.f1 > i.f1){
                            pt = i;
                            break;
                        }
                    }

                    if(pt.f1 != Integer.MAX_VALUE) {
                        for (Tuple2<Integer,Integer> j : edges){
                            collector.collect(new Tuple2<Integer, Integer>(j.f1, pt.f1 + 1));
                        }
                    }
                }
            }
        ).distinct();

        DataSet<Tuple2<Integer, Integer>> deltas = message.coGroup(iterationDelta.getSolutionSet()).where(0).equalTo(0)
            .with(
                new CoGroupFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<Integer, Integer>> j, Iterable<Tuple2<Integer, Integer>> i,
                                        Collector<Tuple2<Integer, Integer>> collector) throws Exception {

                        Tuple2<Integer, Integer> x = new Tuple2<>(0,MAXPOINT);
                        for (Tuple2<Integer,Integer> jiter : j){
                            if(x.f1 > jiter.f1){
                                x = jiter;
                                //System.out.println("sad" + x.f0 + " " + x.f1);
                            }
                        }
                        Tuple2<Integer, Integer> y = new Tuple2<>(0,0);
                        for (Tuple2<Integer,Integer> iiter: i){
                            y = iiter;
                            //System.out.println("happy" + y.f0 + " " + y.f1);
                        }
                        if(x.f1 < y.f1){
                            collector.collect(x);
                        }
                    }
                }
            );

        DataSet<Tuple2<Integer, Integer>> finalVertexLevel = iterationDelta.closeWith(deltas, deltas);

        if (params.has("output")) {
            finalVertexLevel.writeAsCsv(params.get("output"), System.lineSeparator(), " ", OVERWRITE);
            env.execute("Edge List With CoGroup");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            finalVertexLevel.print();
        }
    }
    // *************************************************************************
    //      USER FUNCTIONS
    // *************************************************************************


    private static void setUpIteration(DeltaIteration<?,?> iteration, ExecutionEnvironment env){
        iteration.parallelism(env.getParallelism());
        iteration.setSolutionSetUnManaged(false);
        iteration.name("BfsWithCoGroupInnerIter");
    }
}
