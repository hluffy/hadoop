package MRDemo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PVDemo {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=2){
			System.out.println("please input full path");
			System.exit(0);
		}
		Job job = new Job(new Configuration(),"PVDemo");
		job.setJarByClass(PVDemo.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(PVDemoMap.class);
		job.setReducerClass(PVDemoReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		
	}
	
	public static class PVDemoMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		Text ip = new Text();
		IntWritable one = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] lines = value.toString().split(" ");
			ip.set(lines[0].trim());
			context.write(ip, one);
		}
	}
	
	public static class PVDemoReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		int count = 0;
		Map<String,Integer> map = new HashMap<String, Integer>();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable value : values) {
				sum = sum + value.get();
			}
//			count = count + sum;
			map.put(key.toString(), sum);
		}
		@Override
		protected void cleanup(
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for(String key:map.keySet()){
				sum = sum + map.get(key);
			}
			context.write(new Text(""), new IntWritable(sum));
		}
	}

}
