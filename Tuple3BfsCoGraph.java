import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

/**
 * Created by sanquan.qz on 2017/10/18.
 */
public class Tuple3BfsCoGraph {
    final static int MAXPOINT = 2147483647;

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool paras = ParameterTool.fromArgs(args);

        final int startPoint = paras.getInt("start", 0);

        int nver = paras.getInt("vertex");

        env.setParallelism(paras.getInt("para"));

        String graphfile = paras.get("file");

        //Edge Input
        DataSet<Tuple3<Integer, Integer[], Integer>> verteices = env.readTextFile(graphfile).map(
            new MapFunction<String, Tuple3<Integer, Integer[], Integer>>() {
                @Override
                public Tuple3<Integer, Integer[], Integer> map(String ins) throws Exception {
                    String[] s = ins.split("\t");
                    int start = Integer.parseInt(s[0].trim());
                    String[] edge = s[1].split(" ");
                    int numofedge = Integer.parseInt(edge[0].trim());
                    Integer[] x = new Integer[numofedge];
                    for (int i = 1; i <= numofedge; i++) {
                        x[i - 1] = Integer.parseInt(edge[i].trim());
                    }
                    if(start != startPoint) {
                        return new Tuple3<>(start, x, MAXPOINT);
                    } else{
                        return new Tuple3<>(start, x, 0);
                    }
                }
            }
        );

        //for empty init workset
        DataSet<Tuple3<Integer, Integer[], Integer>> p = verteices.filter(
            new FilterFunction<Tuple3<Integer, Integer[], Integer>>() {
                @Override
                public boolean filter(Tuple3<Integer, Integer[], Integer> value) throws Exception {
                    if(value.f0 .equals(startPoint) ){
                        return true;
                    } else{
                        return false;
                    }
                }
            }
        );

        DeltaIteration<Tuple3<Integer, Integer[], Integer>, Tuple3<Integer, Integer[], Integer>> iteration = verteices.iterateDelta(p, 20, 0);

        setUpIteration(iteration, env);

        DataSet<Tuple2<Integer, Integer>> message = iteration.getWorkset().flatMap(
            new FlatMapFunction<Tuple3<Integer, Integer[], Integer>, Tuple2<Integer, Integer>>() {
                @Override
                public void flatMap(Tuple3<Integer, Integer[], Integer> value,
                                    Collector<Tuple2<Integer, Integer>> out)
                    throws Exception {
                    int level = value.f2;
                    for (Integer i : value.f1){
                        out.collect(new Tuple2<Integer, Integer>(i, level + 1));
                    }
                }
            }
        );
        DataSet<Tuple3<Integer, Integer[], Integer>> delta = message.coGroup(iteration.getSolutionSet()).where(0).equalTo(0).with(
            new CoGroupFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer[], Integer>, Tuple3<Integer,
                Integer[], Integer>>() {
                @Override
                public void coGroup(Iterable<Tuple2<Integer, Integer>> first,
                                    Iterable<Tuple3<Integer, Integer[], Integer>> second,
                                    Collector<Tuple3<Integer, Integer[], Integer>> out) throws Exception {
                       Tuple2<Integer, Integer> tii = null;
                       for (Tuple2<Integer, Integer> i: first){
                           tii = i;
                       }
                       if(tii != null){
                           for (Tuple3<Integer, Integer[], Integer> i : second){
                               if(tii.f1 < i.f2) {
                                   //System.out.println("ffuk " + tii.f0 + " " + tii.f1 + " " + i.f2);
                                   out.collect(new Tuple3<Integer, Integer[], Integer>(i.f0, i.f1, tii.f1));
                               }
                           }
                       }
                }
            }
        ).withForwardedFieldsSecond("f0 -> f0", "f1 -> f1");

        DataSet<Tuple2<Integer, Integer>> ans = iteration.closeWith(delta, delta).project(0,2);

        if(paras.has("output")){
            ans.writeAsCsv(paras.get("output"), System.lineSeparator(), " ", WriteMode.OVERWRITE);
            env.execute("Tuple3BfsCoGraph");
        } else{
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            ans.print();
        }
    }

    //=============================

    private static void setUpIteration(DeltaIteration<?,?> iteration, ExecutionEnvironment env){
        iteration.parallelism(env.getParallelism());
        iteration.setSolutionSetUnManaged(false);
        iteration.name("Tuple3 Bfs");
    }
}
