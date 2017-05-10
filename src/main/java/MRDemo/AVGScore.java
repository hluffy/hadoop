package MRDemo;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AVGScore {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=2){
			System.out.println("please input full path");
			System.exit(0);
		}
		Job job = new Job(new Configuration(),"AVGScore");
		job.setJarByClass(AVGScore.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(AVGMap.class);
		job.setReducerClass(AVGReduce.class);
		
		//如果map输出的key和value与Reduce输出的key和value类型相同，那么map设置key和value的输出类型可以省略
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.waitForCompletion(true);
	}
	
	public static class AVGMap extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		Text name = new Text();
		DoubleWritable score = new DoubleWritable();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
			String[] lines = value.toString().split("\t");
			name.set(new Text(lines[0].trim()));
			score = new DoubleWritable(Double.parseDouble(lines[1].trim()));
			context.write(name, score);
			
		}
	}
	
	public static class AVGReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(key, values, context);
			double sum = 0;
			int count = 0;
			for(DoubleWritable num: values){
				sum = sum + num.get();
				count++;
			}
			double avg = sum/count;
			context.write(key, new DoubleWritable(avg));
			
		}
	}

}
