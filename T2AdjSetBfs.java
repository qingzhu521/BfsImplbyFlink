import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;


public class T2AdjSetBfs {

    final static int MAXPOINT = 2147483647;

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool paras = ParameterTool.fromArgs(args);

        final int startPoint = paras.getInt("start", 0);

        int nver = paras.getInt("vertex");

        env.setParallelism(paras.getInt("para"));

        String graphfile = paras.get("file");

        //Edge Input
        DataSet<Tuple2<Integer, Integer[]>> verteices = env.readTextFile(graphfile).map(
            new MapFunction<String, Tuple2<Integer, Integer[]>>() {
                @Override
                public Tuple2<Integer, Integer[]> map(String ins) throws Exception {
                    String[] s = ins.split("\t");
                    int start = Integer.parseInt(s[0].trim());
                    String[] edge = s[1].split(" ");
                    int numofedge = Integer.parseInt(edge[0].trim());
                    Integer[] x = new Integer[numofedge];
                    for (int i = 1; i <= numofedge; i++) {
                        x[i - 1] = Integer.parseInt(edge[i].trim());
                    }
                    return new Tuple2<>(start, x);
                }
            }
        ).partitionByHash(0);

        //VertexInput
        DataSet<Tuple2<Integer, Integer>> init =
            env.generateSequence(0, nver).map(new LevelAssigner(startPoint)).partitionByHash(0);

        //for empty init workset
        DataSet<Tuple2<Integer, Integer>> p = env.fromElements(new Tuple2<Integer, Integer>(startPoint, 0));

        DeltaIteration<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> iteration = init.iterateDelta(p, 20, 0);

        setUpIteration(iteration, env);

        //if i use join JoinHint.HASH SECOND will cause long term wait
        DataSet<Tuple2<Integer, Integer>> message = iteration.getWorkset().joinWithHuge(verteices).where(0).equalTo(0)
            .with(
                new FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer[]>, Tuple2<Integer, Integer>>() {
                    @Override
                    public void join(Tuple2<Integer, Integer> cand, Tuple2<Integer, Integer[]> verE,
                                     Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                        for (Integer x : verE.f1) {
                            collector.collect(new Tuple2<Integer, Integer>(x, cand.f1 + 1));
                        }
                    }
                }
             );


        DataSet<Tuple2<Integer, Integer>> effmessage = message.distinct();

        DataSet<Tuple2<Integer,Integer>> nextworkset = iteration.getSolutionSet().joinWithTiny(effmessage).where(0)
         .equalTo(0).with(
            new FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                @Override
                public void join(Tuple2<Integer, Integer> sol, Tuple2<Integer, Integer> eff,
                    Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                    if(sol.f1 > eff.f1){
                        collector.collect(eff);
                    }
                }
            }
        );


        DataSet<Tuple2<Integer, Integer>> ans = iteration.closeWith(nextworkset, nextworkset);
        if(paras.has("output")){
            ans.writeAsCsv(paras.get("output"), System.lineSeparator(), " ", WriteMode.OVERWRITE);
            env.execute("f th job");
        } else{
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            ans.print();
        }
    }

    //=============================
    public static final class LevelAssigner implements MapFunction<Long, Tuple2<Integer,Integer>> {
        // for we cannot use index to get data serially so we need a manual index
        private int startpoint;
        public LevelAssigner(int inn){
            startpoint = inn;
        }
        @Override
        public Tuple2<Integer,Integer> map(Long vertex){
            if(vertex.intValue() == (startpoint)){
                return new Tuple2<>(startpoint,0);
            } else {
                return new Tuple2<>(vertex.intValue(), Integer.MAX_VALUE);
            }
        }
    }

    private static void setUpIteration(DeltaIteration<?,?> iteration, ExecutionEnvironment env){
        iteration.parallelism(env.getParallelism());
        iteration.setSolutionSetUnManaged(false);
        iteration.name("T2AdjSetBfs");
    }
}