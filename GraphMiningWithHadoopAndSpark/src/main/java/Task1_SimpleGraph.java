import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class Task1_SimpleGraph extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        String input = args[0];
        String output = args[1];

        Configuration conf = getConf();

        Job job = Job.getInstance(conf);

        job.setJarByClass(RemoveDuplicateEdgeMapper.class);

        job.setMapperClass(RemoveDuplicateEdgeMapper.class);
        job.setReducerClass(RemoveDuplicateEdgeReducer.class);

        job.setMapOutputKeyClass(IntPairWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(IntPairWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job. waitForCompletion(true);

        return 0;
    }

    public static class RemoveDuplicateEdgeMapper extends Mapper<Object, Text, IntPairWritable, NullWritable> {

        IntPairWritable ok = new IntPairWritable();
        NullWritable ov = NullWritable.get();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer st = new StringTokenizer(value.toString());
            int u = Integer.parseInt(st.nextToken());
            int v = Integer.parseInt(st.nextToken());

            // self-loop 제거
            if (u < v) ok.set(u, v);
            else if (u > v) ok.set(v, u);

            context.write(ok, ov);
        }
    }

    public static class RemoveDuplicateEdgeReducer extends Reducer<IntPairWritable, NullWritable, IntPairWritable, NullWritable> {

        NullWritable ov = NullWritable.get();

         @Override
        protected void reduce(IntPairWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, ov);
        }
    }
}