package CarsDemo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

public class SexCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=2){
			System.out.println("please input full path");
			System.exit(0);
		}
		Job job = new Job(new Configuration(),"SexCount");
		job.setJarByClass(SexCount.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(CountMap.class);
		job.setReducerClass(CountReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.waitForCompletion(true);
	}
	
	public static class CountMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		Text sex = new Text();
		IntWritable one = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
			String[] lines = value.toString().split("\t");
			if(lines.length>38&&lines[38]!=null&&!lines[38].trim().equals("")){
				sex.set(lines[38].trim());
				context.write(sex, one);
			}
		}
	}
	
	public static class CountReduce extends Reducer<Text, IntWritable, Text, DoubleWritable>{
		int total = 0;//用于统计男女总人数
		Map<String,Integer> map = new HashMap<String, Integer>();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(key, values, context);
			int sum = 0;//分别分别统计男性女性买车人数
			for(IntWritable value:values){
				sum = sum + value.get();
			}
			map.put(key.toString(), sum);//将<性别，人数>添加到map当中
			total = total + sum;
		}
		
		//cleanup方法在reducer之后执行，并且只执行一次
		protected void cleanup(org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,DoubleWritable>.Context context) throws IOException ,InterruptedException {
			Set<String> keySet = map.keySet();
			for(String sex:keySet){
				int count = map.get(sex);//通过性别获取买车人数
				double percent = count/total;
				context.write(new Text(sex), new DoubleWritable(percent));
			}
		};
	}

}
