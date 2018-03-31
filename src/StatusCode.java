/**
 * Created by duwenqin123 on 9/26/17.
 */

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StatusCode {


    public static class StatusCodeMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Text word = new Text();
            Pattern p = Pattern.compile("\\d{4}:(\\d{2}).*?\".*?(\\d+) ");
            Matcher m = p.matcher(value.toString());
            if (m.find()) {
                String time = m.group(1);
                String status = m.group(2);
                word.set(time + "#" + status);
                context.write(word, one);
            }
        }
    }

    public static class StatusCodeCombiner extends
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

    public static class StatusCodePartitioner extends HashPartitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class StatusCodeReducer extends
            Reducer<Text, IntWritable, Text, Text> {
        private MultipleOutputs mos;
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            mos = new MultipleOutputs<>(context);
        }

        static TreeMap<String, Integer> statusToCountMap = new TreeMap<>();
        static TreeMap<Integer, TreeMap<String, Integer>> timeToStatusCount = new TreeMap<>();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{

            int sum = 0;
            Integer time = Integer.parseInt(key.toString().split("#")[0]);
            String statusCode = key.toString().split("#")[1];
            for(IntWritable val:values)
                sum += val.get();

            if (!statusToCountMap.containsKey(statusCode)) {
                statusToCountMap.put(statusCode, 0);
            }
            statusToCountMap.put(statusCode, statusToCountMap.get(statusCode) + sum);

            if (!timeToStatusCount.containsKey(time)) {
                timeToStatusCount.put(time, new TreeMap<String, Integer>());
            }
            timeToStatusCount.get(time).put(statusCode, sum);

        }
        public void cleanup(Context context) throws IOException,
                InterruptedException {
            String[] statusCodeArray={"200", "400", "404"};
            Text timeText = new Text();
            String fileName="1.txt";


            for (int i = 0; i < statusCodeArray.length; i++) {
                mos.write(new Text(statusCodeArray[i]+":"+Integer.toString(statusToCountMap.containsKey(statusCodeArray[i])?
                        statusToCountMap.get(statusCodeArray[i]):0)),new Text(),fileName);
            }
            for(int i=0;i<24;i++) {
                StringBuilder codeToCount = new StringBuilder();
                timeText.set(i+":00-"+(i+1)+":00");
                if (!timeToStatusCount.containsKey(i)) {
                    codeToCount.append("200:0 400:0 404:0");
                    mos.write(timeText, new Text(codeToCount.toString()),fileName);
                    continue;
                }
                TreeMap<String,Integer> timeMap=timeToStatusCount.get(i);
                for (int j = 0; j < statusCodeArray.length; j++) {
                    codeToCount.append(statusCodeArray[j]+":"+(
                            timeMap.containsKey(statusCodeArray[j])?
                                    timeMap.get(statusCodeArray[j]):0)+" ");
                }
                mos.write(timeText,new Text(codeToCount.toString()),fileName);
            }
            mos.close();
        }

    }


    public static int main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(StatusCode.class);
        job.setJobName("inverted index");

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(StatusCodeMapper.class);
        job.setCombinerClass(StatusCodeCombiner.class);
        job.setPartitionerClass(StatusCodePartitioner.class);
        job.setReducerClass(StatusCodeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(MyTextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true))?0:1;
    }
}

