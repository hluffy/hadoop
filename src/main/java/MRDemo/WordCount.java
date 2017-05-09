package MRDemo;

import java.io.IOException;

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


public class WordCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=0){
			System.out.println("Plase input full path!");
			System.exit(0);
		}
		Job job = new Job(new Configuration(),"Word Count");
		//将WordCount类打包运行
		job.setJarByClass(WordCount.class);
		//设置输入路径
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		//设置输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//设置运行map函数的类
		job.setMapperClass(WordCountMap.class);
		//设置运行reduce函数的类
		job.setReducerClass(WordCountReduce.class);
		//设置map输出的key的数据类型
		job.setMapOutputKeyClass(Text.class);
		//设置map输出的value的数据类型
		job.setMapOutputValueClass(IntWritable.class);
		//设置reduce最终输出的key的数据类型
		job.setOutputKeyClass(Text.class);
		//设置reduce最终输出的value的数据类型
		job.setOutputValueClass(IntWritable.class);
		
		//提交job
		job.waitForCompletion(true);
	}
	/**
	 * KEYIN 每行文本的偏移量类型
	 * VALUEIN 每行文本内容的数据类型
	 * KEYOUT Map输出的中间结果的key的数据类型
	 * VALUEOUT Map输出的中间结果的value的数据类型
	 * 快捷键 alt+shift+l生成返回值及类型
	 * @author xhan
	 *
	 */
	public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
			String[] lines = value.toString().split(" ");
			for(String info:lines){
				//输出中间结果<hello,1> <world,1> <hello,1> <hadoop,1>
				context.write(new Text(info), new IntWritable(1));
			}
		}
		
	}
	
	/**
	 * KEYIN 输入给Reduce处理的key的数据类型
	 * VALUEIN 输入给Reduce处理的value的数据类型
	 * KEYOUT 输出的最终结果的key的数据类型
	 * VALUEOUT 输出的最终结果的value的数据类型
	 * @author xhan
	 *
	 * Map产生的中间结果需要经过shuffle阶段进行处理，处理之后成为以下形式：
	 * <hello,{1,1}> <world,{1}> <hadoop,{1}>
	 */
	public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(key, values, context);
			int sum = 0;
			for (IntWritable count : values) {
				sum = sum + count.get();
			}
			//最终的输出结果<hello,2> <world,1> <hadoop,1>
			context.write(key, new IntWritable(sum));
		}
	}

}
