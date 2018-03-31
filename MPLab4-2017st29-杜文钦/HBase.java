package example;
/**
 * Created by lonhuen on 4/17/17.
 */

//import InvertedIndexer.sort;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.GenericOptionsParser;

public class HBase {
    static String tableName = "Wuxia";
    static Configuration HBASE_CONFIG = HBaseConfiguration.create();
    /*output (word@file,frequency)*/
    public static class HBaseMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            /*TODO delete the .txt.segmented*/
            String fileName = fileSplit.getPath().getName().split("\\.[t|T][x|X][t|T]")[0];
            Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString());
            for (; itr.hasMoreTokens();) {
                word.set(itr.nextToken() + "#" + fileName);
                context.write(word,one);
            }
        }
    }

    /*combine the map result*/
    public static class HBaseCombiner extends
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

    public static class HBasePartitioner extends HashPartitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class HBaseReducer extends
            Reducer<Text, IntWritable, Text, Text> {

        static List<String> postingList = new ArrayList<String>();
        static long wordsum = 0;
        static Text preword = new Text(" ");

        private Text word = new Text();
        private Text file = new Text();
        private String tmpCell = new String();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{

            /*calculate the sum of word#file*/
            int sum = 0;
            word.set(key.toString().split("#")[0]);
            tmpCell = key.toString().split("#")[1];
            for(IntWritable val:values)
                sum += val.get();
            /* remember to add ";" when output*/
            file.set(tmpCell + ":" + sum);

            /*calculate the sum of word in all the file*/
            wordsum += sum;


            /*finish a word processing*/
            if(!preword.equals(word) && !preword.equals(new Text(" "))){

                StringBuilder out = new StringBuilder();
                long size = postingList.size();
                double average = wordsum / (double) size;

                HTable table =new HTable(HBASE_CONFIG, tableName);
                Put put = new Put(preword.getBytes());
                put.add(Bytes.toBytes("datas"), Bytes.toBytes("average"),
                        Bytes.toBytes(String.format("%.2f", average)));
                table.put(put);
                table.close();
                /*TODO the digit should be limited I think*/
                DecimalFormat df = new DecimalFormat("0.00");
                out.append(df.format(average));
                out.append(",");

                for (String p : postingList) {
                    out.append(p);
                    out.append(";");
                }

                /*delete the last ";"*/
                out.deleteCharAt(out.length() - 1);

                /*output the result*/
                if (wordsum > 0)
                    context.write(preword, new Text(out.toString()));

                /*reset the wordsum*/
                wordsum = 0;

                /*reset the list*/
                postingList = new ArrayList<String>();
            }

            preword = new Text(word);
            postingList.add(file.toString());
        }
        public void cleanup(Context context) throws IOException,
                InterruptedException {
            StringBuilder out = new StringBuilder();
            long size = postingList.size();
            if(size > 0) {
                double average = wordsum / (double) size;
                HTable table =new HTable(HBASE_CONFIG, tableName);
                Put put = new Put(preword.getBytes());
                put.add(Bytes.toBytes("datas"), Bytes.toBytes("average"),
                        Bytes.toBytes(String.format("%.2f", average)));
                table.put(put);
                table.close();
                out.append(average);
                out.append(",");

                for (String p : postingList) {
                    out.append(p);
                    out.append(";");
                }
                /*delete the last ";"*/
                out.deleteCharAt(out.length() - 1);
                /*output the result*/
                if (wordsum > 0)
                    context.write(preword, new Text(out.toString()));
            }
        }

    }


    public static void main(String[] args) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(HBASE_CONFIG);
        if(admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor=new HColumnDescriptor("datas");
        tableDescriptor.addFamily(columnDescriptor);
        admin.createTable(tableDescriptor);

        String[] otherArgs = new GenericOptionsParser(HBASE_CONFIG, args).getRemainingArgs();
        if(otherArgs.length != 2) {
            System.err.println("Usage: HBase <input> <output>");
            System.exit(2);
        }

        Job job = new Job(HBASE_CONFIG, "HBase");
        job.setJarByClass(HBase.class);
        job.setJobName("HBase lab");

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(HBaseMapper.class);
        job.setCombinerClass(HBaseCombiner.class);
        job.setPartitionerClass(HBasePartitioner.class);
        job.setReducerClass(HBaseReducer.class);
        job.setNumReduceTasks(5);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        /*output value for map is IntWritable rather than Text*/
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}

