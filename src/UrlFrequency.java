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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UrlFrequency{

    public static class UrlFrequencyMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Text word = new Text();
            Pattern p = Pattern.compile(":(\\d{2}:\\d{2}:\\d{2}).*?/(.*?/.*?/.*?) ");
            Matcher m = p.matcher(value.toString());
            if (m.find()) {
                String time = m.group(1);
                String url = m.group(2);
                word.set(url + "#" + time);
                context.write(word, one);
            }
        }
    }

    /*combine the map result*/
    public static class UrlFrequencyCombiner extends
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

    public static class UrlFrequencyPartitioner extends HashPartitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class UrlFrequencyReducer extends
            Reducer<Text, IntWritable, Text, Text> {
        private MultipleOutputs mos;
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            mos = new MultipleOutputs<>(context);
        }
        static TreeMap<String, Integer> urlToCountMap = new TreeMap<>();
        static TreeMap<String, TreeMap<String, Integer>> urlToTimeCount = new TreeMap<>();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{

            int sum = 0;
            String url = key.toString().split("#")[0];
            String time= key.toString().split("#")[1];
            for(IntWritable val:values)
                sum += val.get();
            if (!urlToCountMap.containsKey(url)) {
                urlToCountMap.put(url, 0);
            }
            urlToCountMap.put(url, urlToCountMap.get(url) + sum);

            if (!urlToTimeCount.containsKey(url)) {
                urlToTimeCount.put(url, new TreeMap<String, Integer>());
            }
            urlToTimeCount.get(url).put(time, sum);

        }
        public void cleanup(Context context) throws IOException,
                InterruptedException {
            Text timeText = new Text();

            Set<Map.Entry<String, Integer>> entrySet = urlToCountMap.entrySet();
            for (Map.Entry<String, Integer> entry : entrySet) {
                String key = entry.getKey();
//                String fileName=key+".txt";
                String fileName = key.toString().replace('/', '-')+".txt";
                mos.write(new Text(key+":"+entry.getValue()),new Text(""),fileName);

                //context.write(new Text(key), new IntWritable(entry.getValue()));
                //builder.append(String.valueOf(entry.getValue()) + "\n");

                TreeMap<String, Integer> timeToCountVal = urlToTimeCount.get(key);
                Set<Map.Entry<String, Integer>> entrySet1 = timeToCountVal.entrySet();
                for (Map.Entry<String, Integer> entry1 : entrySet1) {
                    //builder.append(entry1.getKey() + "\t" + entry1.getValue() + "\n");
                    mos.write(new Text(entry1.getKey()),new Text(entry1.getValue()+""),fileName);
                }
            }
            mos.close();

        }
    }


    public static int main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(UrlFrequency.class);
        job.setJobName("inverted index");

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(UrlFrequencyMapper.class);
        job.setCombinerClass(UrlFrequencyCombiner.class);
        job.setPartitionerClass(UrlFrequencyPartitioner.class);
        job.setReducerClass(UrlFrequencyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(MyTextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        return (job.waitForCompletion(true))?0:1;
    }
}


