/**
 * Created by duwenqin123 on 9/27/17.
 */

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.map.HashedMap;
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

public class UrlAccessAvg{

    public static class UrlAccessAvgMapper extends
            Mapper<Object, Text, Text, Text> {

       // private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Text word = new Text();
            Pattern p = Pattern.compile(":(\\d{2}).*?/(.*?) .*?\\d{3} \\d+ (\\d+)");
            Matcher m = p.matcher(value.toString());
            if (m.find()) {
                String time = m.group(1);
                String url = m.group(2);
                word.set(url + "#" + time);
                String resTime = m.group(3);
                context.write(word,new Text(resTime+"#1"));
            }
        }
    }

    /*combine the map result*/
    public static class UrlAccessAvgCombiner extends
            Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

//            int sum = 0;
            int resTimeSum=0,countSum=0;
            for (Text val : values) {
                Integer resTime=Integer.parseInt(val.toString().split("#")[0]);
                Integer count=Integer.parseInt(val.toString().split("#")[1]);
                resTimeSum+=resTime*count;
                countSum+=count;
            }
//            result.set(sum);
            context.write(key,new Text(resTimeSum+"#"+countSum));
        }
    }

    public static class UrlAccessAvgPartitioner extends HashPartitioner<Text, Text> {
        public int getPartition(Text key, Text value, int numReduceTasks) {
            String term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class UrlAccessAvgReducer extends
            Reducer<Text, Text, Text, Text> {
        private MultipleOutputs mos;
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            mos = new MultipleOutputs<>(context);
        }
        static TreeMap<String, String> urlToCountMap = new TreeMap<>();
        static TreeMap<String, TreeMap<Integer, Double>> urlToTimeCount = new TreeMap<>();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{

            double resTimeSum=0,countSum=0;
            for (Text val : values) {
                double resTime=Double.parseDouble(val.toString().split("#")[0]);
                Integer count=Integer.parseInt(val.toString().split("#")[1]);
                resTimeSum+=resTime*count;
                countSum+=count;
            }
            double resTimeAvg=resTimeSum/countSum;
            String url = key.toString().split("#")[0];
            String time= key.toString().split("#")[1];

            if (!urlToCountMap.containsKey(url)) {
                urlToCountMap.put(url, "0#0");
            }
            //int resTimeSum1=0,countSum1=0;
            String sumAndCount = urlToCountMap.get(url);
            Double resTime=Double.parseDouble(sumAndCount.split("#")[0]);
            Double count=Double.parseDouble(sumAndCount.split("#")[1]);
            urlToCountMap.put(url,(resTimeSum+resTime)+"#"+(countSum+count));

            if (!urlToTimeCount.containsKey(url)) {
                urlToTimeCount.put(url, new TreeMap<Integer, Double>());
            }
            urlToTimeCount.get(url).put(Integer.parseInt(time), resTimeAvg);
        }
        public void cleanup(Context context) throws IOException,
                InterruptedException {
//            String[] UrlAccessAvgArray={"200", "400", "404"};
            Text timeText = new Text();
            //Text codeCountText = new Text();
//            for (int i = 0; i < UrlAccessAvgArray.length; i++) {
//                context.write(new Text(UrlAccessAvgArray[i]),new Text(Integer.toString(urlToCountMap .containsKey(UrlAccessAvgArray[i])?
//                        urlToCountMap .get(UrlAccessAvgArray[i]):0)));
//            }
            Set<Map.Entry<String, String>> entrySet = urlToCountMap.entrySet();
            for (Map.Entry<String, String> entry : entrySet) {
                String key=entry.getKey();
                String value=entry.getValue();
                double total = Double.parseDouble(value.split("#")[0]);
                double count = Double.parseDouble(value.split("#")[1]);
                double avg=total/count;

                String fileName = key.toString().replace('/', '-')+".txt";
                mos.write(new Text(key+":"+avg),new Text(""),fileName);

                //context.write(new Text(key), new IntWritable(entry.getValue()));
//                builder.append(avg + "\n");

                //TreeMap<String, Double> timeToCountVal = urlToTimeCount.get(key);
                //Set<Map.Entry<String, Double>> entrySet1 = timeToCountVal.entrySet();
                for (int i = 0; i < 24; i++) {
//                    builder.append(i + ":00-" + (i + 1) + ":00\t");
                    timeText.set(i + ":00-" + (i + 1) + ":00");
                    TreeMap<Integer, Double> timeToCountVal = urlToTimeCount.get(key);
                    mos.write(timeText,new Text((timeToCountVal.containsKey(i) ? timeToCountVal.get(i) : 0)+"")
                            ,fileName);

                }
            }
            mos.close();
        }

    }


    public static int main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(UrlAccessAvg.class);
        job.setJobName("UrlAccessAvgR");

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(UrlAccessAvgMapper.class);
        job.setCombinerClass(UrlAccessAvgCombiner.class);
        job.setPartitionerClass(UrlAccessAvgPartitioner.class);
        job.setReducerClass(UrlAccessAvgReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(MyTextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        return (job.waitForCompletion(true))?0:1;
    }
}


