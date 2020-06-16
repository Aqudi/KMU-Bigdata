package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class Task1 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Task1(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        Job job = Job.getInstance(conf);

        job.setJarByClass(RemoveDuplicateEdgeMapper.class);

        job.setMapperClass(RemoveDuplicateEdgeMapper.class);
        job.setReducerClass(RemoveDuplicateEdgeReducer.class);

        job.setMapOutputKeyClass(IntPairWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);

        return 0;
    }

    public static class RemoveDuplicateEdgeMapper extends Mapper<Object, Text, IntPairWritable, NullWritable> {

        IntPairWritable ok = new IntPairWritable();
        NullWritable ov = NullWritable.get();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // 토크나이저로 Text 파일을 받아와 메타 정보를 필터링하고 Edge 를 emit
            StringTokenizer st = new StringTokenizer(value.toString());
            String su = st.nextToken();
            if (su.startsWith("%") || su.startsWith("#")) {
                return;
            }

            int u = Integer.parseInt(su);
            int v = Integer.parseInt(st.nextToken());

            // self-loop 제거
            if (u < v) ok.set(u, v);
            else if (u > v) ok.set(v, u);

            // IntPairWritable 로 Key 를 잡아 Edge 기준으로 Grouping 되도록 한다.
            context.write(ok, ov);
        }
    }

    public static class RemoveDuplicateEdgeReducer extends Reducer<IntPairWritable, NullWritable, IntWritable, IntWritable> {

        IntWritable ok = new IntWritable();
        IntWritable ov = new IntWritable();

        @Override
        protected void reduce(IntPairWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            // Edge 기준으로 Grouping 했으므로 그대로 출력해준다.
            // 같은 Edge 들은 Key 로 묶였으니 바로 write 해주면 된다.
            ok.set(key.u);
            ov.set(key.v);
            context.write(ok, ov);
        }
    }
}