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

/**
 * 统计第一季度每位手机用户的上网总量，通话总时间，短信总量
 * @author xhan
 *
 */
public class MobileMR {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=2){
			System.out.println("please input full path");
			System.exit(0);
		}
		
		Job job = new Job(new Configuration(),"MobileMr");
		job.setJarByClass(MobileMR.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MobileMapper.class);
		job.setReducerClass(MobileReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mobile.class);
		
		job.waitForCompletion(true);
	}
	
	public static class MobileMapper extends Mapper<LongWritable, Text, Text, Mobile>{
		Text phoneNum = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Mobile>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
			String[] lines = value.toString().split("\t");
			if(lines.length==4){
				phoneNum.set(lines[0].trim());//手机号
				Mobile mobile = new Mobile(Integer.parseInt(lines[1].trim()),
						Integer.parseInt(lines[2].trim()),
						Integer.parseInt(lines[3].trim()));
				context.write(phoneNum, mobile);
			}
		}
	}
	
	/**
	 * map的中间结果通过shuffle处理变成
	 * <1213423,{Moblie1,Mobile2,Mobile3}>
	 * @author xhan
	 *
	 */
	public static class MobileReduce extends Reducer<Text, Mobile, Text, Mobile>{
		@Override
		protected void reduce(Text key, Iterable<Mobile> values,
				Reducer<Text, Mobile, Text, Mobile>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(key, values, context);
			int internet = 0;
			int callTime = 0;
			int mailCount = 0;
			for(Mobile mobile:values){
				internet = internet + mobile.getInternet();
				callTime = callTime + mobile.getCallTime();
				mailCount = mailCount + mobile.getMailCount();
			}
			Mobile mobileData = new Mobile(internet,callTime,mailCount);
			context.write(key, mobileData);
		}
	}

}
