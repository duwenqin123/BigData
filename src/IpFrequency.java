/**
 * Created by duwenqin123 on 9/27/17.
 */


import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class IpFrequency{
    //public static String myOutputFormat="/home/duwenqin123/IpFrequency/output";
    public static class IpFrequencyMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Text word = new Text();
            Pattern p = Pattern.compile("(\\d{3}\\.\\d{2}\\.\\d{2}\\.\\d{2}).*?:(\\d{2})");
            Matcher m = p.matcher(value.toString());
            if (m.find()) {
                String ip = m.group(1);
                String time = m.group(2);
                word.set(ip + "#" + time);
                context.write(word, one);
            }
        }
    }

    /*combine the map result*/
    public static class IpFrequencyCombiner extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class IpFrequencyPartitioner extends HashPartitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class IpFrequencyReducer extends
            Reducer<Text, IntWritable, Text, Text> {
        private MultipleOutputs mos;
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            mos = new MultipleOutputs<>(context);
        }


        static TreeMap<String, Integer> ipToCountMap = new TreeMap<>();
        static TreeMap<String, TreeMap<Integer, Integer>> ipToTimeCount = new TreeMap<>();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{


            int sum = 0;
            String ip = key.toString().split("#")[0];
            Integer time= Integer.parseInt(key.toString().split("#")[1]);
            for(IntWritable val:values)
                sum += val.get();
            if (!ipToCountMap.containsKey(ip)) {
                ipToCountMap.put(ip, 0);
            }
            ipToCountMap.put(ip, ipToCountMap.get(ip) + sum);

            if (!ipToTimeCount.containsKey(ip)) {
                ipToTimeCount.put(ip, new TreeMap<Integer, Integer>());
            }
            ipToTimeCount.get(ip).put(time, sum);

        }
        public void cleanup(Context context) throws IOException,
                InterruptedException {
            Set<Map.Entry<String, Integer>> entrySet = ipToCountMap.entrySet();
            for (Map.Entry<String, Integer> entry : entrySet) {
                StringBuilder builder = new StringBuilder();
                String key = entry.getKey();

                String fileName=key+".txt";
                mos.write(new Text(key+":"+entry.getValue()),new Text(""),fileName);


                for (int i = 0; i < 24; i++) {
                    String hourWindow=i + ":00-" + (i + 1) + ":00\t";
                    TreeMap<Integer, Integer> timeToCountVal = ipToTimeCount.get(key);
                    Integer timeCount=timeToCountVal.containsKey(i) ? timeToCountVal.get(i) : 0;
                    mos.write(new Text(hourWindow),new Text(String.valueOf(timeCount)),fileName);
                }
                context.write(new Text(key),new Text(builder.toString()));
            }
            mos.close();
        }

    }


    public static int main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(IpFrequency.class);
        job.setJobName("inverted index");

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(IpFrequencyMapper.class);
        job.setCombinerClass(IpFrequencyCombiner.class);
        job.setPartitionerClass(IpFrequencyPartitioner.class);
        job.setReducerClass(IpFrequencyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        /*output value for map is IntWritable rather than Text*/
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(MyTextOutputFormat.class);
//        job.setOutputFormatClass(MyMultipleOutputFormat.class);
        // LazyOutputFormat.setOutputFormatClass(job,MyMultipleOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true)?0:1;
    }
}

