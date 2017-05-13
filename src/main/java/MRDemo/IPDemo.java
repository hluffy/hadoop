package MRDemo;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

public class IPDemo {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=2){
			System.out.println("please input full path");
			System.exit(0);
		}
		Job job = new Job(new Configuration(),"IPDemo");
		job.setJarByClass(IPDemo.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(IPDemoMap.class);
		job.setReducerClass(IPDemoReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		
	}
	
	public static class IPDemoMap extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] lines = value.toString().split(" ");
			if(lines.length==8){
				context.write(new Text(lines[6]), new Text(lines[0]));
			}
		}
	}
	
	public static class IPDemoReduce extends Reducer<Text, Text, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Set<String> set = new HashSet<String>();
			for (Text value : values) {
				set.add(value.toString());
			}
			if(set.size()>0){
				context.write(key, new IntWritable(set.size()));
			}
		}
	}

}
