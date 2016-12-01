题目3：编写MapReduce，统计这两个文件

`/user/hadoop/mapred_dev_double/ip_time`

`/user/hadoop/mapred_dev_double/ip_time_2`

当中，重复出现的IP的数量(40分)

---
加分项：

1. 写出程序里面考虑到的优化点，每个优化点+5分。
2. 额外使用Hive实现，+5分。
3. 额外使用HBase实现，+5分。


package wc;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class TextCount2 {
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable , Text,
			Text, IntWritable>{
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] splits = line.split("\t");
				output.collect(new Text(splits[0]),one);
			
		}
	}
	
	
	
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, 
		Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while(values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	
	
	
	public static class MyPartitioner extends MapReduceBase 
		implements Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int reduceNum) {
			if (key.toString().charAt(0) - 'a' >= 0) {
				return 1;
			} else {
				return 0;
			}
		}	
	}
		
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(wordcount.class);
		conf.setJobName("TextCount2");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setPartitionerClass(MyPartitioner.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setNumReduceTasks(2);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}
第一个：Reduce output records=1218
第二个：Reduce output records=1249
将两个数据输出到统一个文件夹中，在讲上述代码运行一遍，得出结果：Reduce output records=1917
最中结果：1218+1249-1917=550
