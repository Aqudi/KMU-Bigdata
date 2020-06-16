package bigdata;

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

public class Task2 extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {

        String input = args[0];
        String output = args[1];

        Configuration conf = getConf();

        Job job = Job.getInstance(conf);

        job.setJarByClass(DegreeComputeMapper.class);

        job.setMapperClass(DegreeComputeMapper.class);
        job.setReducerClass(DegreeComputeReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntPairWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);

        return 0;
    }

    public static class DegreeComputeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        IntWritable ou = new IntWritable();
        IntWritable ov = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer st = new StringTokenizer(value.toString());

            ou.set(Integer.parseInt(st.nextToken()));
            ov.set(Integer.parseInt(st.nextToken()));

            context.write(ou, ov);
            context.write(ov, ou);
        }
    }

    public static class DegreeComputeReducer extends Reducer<IntWritable, IntWritable, IntPairWritable, NullWritable> {

        NullWritable ov = NullWritable.get();
        IntPairWritable ok = new IntPairWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;

            for(IntWritable value : values) {
                count += 1;
            }

            ok.set(key.get(), count);
            context.write(ok, ov);
        }
    }
}