package com.caojingchen.SortedKeyValue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortedKeyValue {


	public static class KVMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text tkey = new Text();
		private Text tvalue = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] logs = value.toString().split(" ");
			if (logs[0].matches("([a-z]+)")) {
				tkey.set(logs[1]);
				tvalue.set(logs[0]);
			} else {
				tkey.set(logs[0]);
				tvalue.set(logs[1]);
			}
			context.write(tkey, tvalue);
		}
	}
	public static class MyComparator extends WritableComparator{

		public MyComparator(){
			super(Text.class,true);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			return -a.compareTo(b);
		}

		
	}

	public static class KVReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for(Text value:values){
				context.write(key, value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err
					.println("Usage: com.caojingchen.SortedKeyValue.SortedKeyValue <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "SortedKeyValue");
		job.setJarByClass(SortedKeyValue.class);
		job.setMapperClass(KVMapper.class);
		job.setReducerClass(KVReducer.class);
		job.setSortComparatorClass(MyComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputDirRecursive(job, true);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
