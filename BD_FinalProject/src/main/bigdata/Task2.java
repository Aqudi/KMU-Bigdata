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

public class Task2 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Task2(), args);
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

        job.setJarByClass(DegreeComputeMapper.class);

        job.setMapperClass(DegreeComputeMapper.class);
        job.setReducerClass(DegreeComputeReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntPairWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);

        return 0;
    }

    public static class DegreeComputeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        IntWritable ou = new IntWritable();
        IntWritable ov = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Degree 를 계산하기 위해 Edge 들을 받아온다.
            StringTokenizer st = new StringTokenizer(value.toString());

            ou.set(Integer.parseInt(st.nextToken()));
            ov.set(Integer.parseInt(st.nextToken()));
            // System.out.println(ou.get() + " " + ov.get());

            // Undirected graph 이므로 u,v v,u 두 개의 튜플을 write 한다.
            context.write(ou, ov);
            context.write(ov, ou);
        }
    }

    public static class DegreeComputeReducer extends Reducer<IntWritable, IntWritable, IntPairWritable, NullWritable> {

        IntPairWritable ok = new IntPairWritable();
        NullWritable ov = NullWritable.get();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // U, V 를 기준으로 모인 Value 의 개수를 센다.
            int count = 0;
            // System.out.println(key.get());
            for (IntWritable value : values) {
                count += 1;
            }

            // vertex, count 순으로 write 해준다.
            ok.set(key.get(), count);
            context.write(ok, ov);
        }
    }
}