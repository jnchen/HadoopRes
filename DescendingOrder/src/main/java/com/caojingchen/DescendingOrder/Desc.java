package com.caojingchen.DescendingOrder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 降序workcount
 *
 */
public class Desc {
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumCombiner extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, IntWritable, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(result, key);
		}
	}

	public static class MyKeyType implements WritableComparable<MyKeyType> {
		private IntWritable key = null;

		public MyKeyType() {
			key = new IntWritable();
		}

		public MyKeyType(IntWritable key) {
			this.key = key;
		}

		public IntWritable get() {
			return this.key;
		}

		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			key.readFields(arg0);
		}

		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			key.write(arg0);
		}

		public int compareTo(MyKeyType o) {
			// TODO Auto-generated method stub
			return -key.compareTo(o.get());
		}

	}

	public static class SortMapper extends
			Mapper<IntWritable, Text, MyKeyType, Text> {

		@Override
		protected void map(IntWritable key, Text value,
				Mapper<IntWritable, Text, MyKeyType, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			MyKeyType mykey = new MyKeyType(key);
			context.write(mykey, value);
		}

	}

	public static class SortReducer extends
			Reducer<MyKeyType, Text, Text, IntWritable> {

		@Override
		protected void reduce(MyKeyType key, Iterable<Text> values,
				Reducer<MyKeyType, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Text value : values) {
				context.write(value, key.get());
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "desc word count-count");
		job.setJarByClass(Desc.class);
		job.setMapperClass(TokenizerMapper.class);

		job.setReducerClass(IntSumReducer.class);
		job.setCombinerClass(IntSumCombiner.class);// 本地reduce
													// 输出的keyvalue必须和输入的keyvalue一样
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		Path tmp = new Path("wordcount-temp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		FileOutputFormat.setOutputPath(job, tmp);
		try {
			if (job.waitForCompletion(true)) {
				Job sortJob = Job.getInstance(conf, "desc word count-sort");
				sortJob.setJarByClass(Desc.class);
				sortJob.setMapperClass(SortMapper.class);
				sortJob.setReducerClass(SortReducer.class);
				sortJob.setMapOutputKeyClass(MyKeyType.class);
				sortJob.setMapOutputValueClass(Text.class);
				sortJob.setOutputKeyClass(Text.class);
				sortJob.setOutputValueClass(IntWritable.class);
				sortJob.setInputFormatClass(SequenceFileInputFormat.class);
				FileInputFormat.addInputPath(sortJob, tmp);
				FileOutputFormat.setOutputPath(sortJob, new Path(
						otherArgs[otherArgs.length - 1]));
				System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
			}
		} finally {
			FileSystem.get(conf).deleteOnExit(tmp);
		}
	}
}
