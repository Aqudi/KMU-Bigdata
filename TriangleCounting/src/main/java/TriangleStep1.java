import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class TriangleStep1 extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];

        Job job = Job.getInstance(getConf());
        job.setJarByClass(TriangleStep1.class);

        job.setMapperClass(TS1Map.class);
        job.setReducerClass(TS1Reduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntPairWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);

        return 0;
    }

    public static class TS1Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        IntWritable ov = new IntWritable();
        IntWritable ou = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String k = key.toString();
            StringTokenizer st = new StringTokenizer(k);

            if (st.hasMoreTokens()) ov.set(Integer.parseInt(st.nextToken()));
            if (st.hasMoreTokens()) ou.set(Integer.parseInt(st.nextToken()));

            if (ov.get() > ou.get()) context.write(ou, ov);
            else context.write(ov, ou);

        }
    }

    public static class TS1Reduce extends Reducer<IntWritable, IntWritable, IntPairWritable, IntWritable> {

        IntPairWritable ok = new IntPairWritable();
        IntWritable ov = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            List<Integer> neighbors = new ArrayList<>();
            for (IntWritable v : values) {
                neighbors.add(v.get());
            }

            for (int i = 0; i < neighbors.size(); i++) {
                for (Integer neighbor : neighbors) {
                    if (neighbors.get(i) < neighbor) {
                        ok.set(neighbors.get(i), neighbor);
                        context.write(ok, ov);
                    }
                }
            }
        }
    }

}
