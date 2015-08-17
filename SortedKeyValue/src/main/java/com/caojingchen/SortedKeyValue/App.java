package com.caojingchen.SortedKeyValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class App {

	public static class KeyValueMapper extends
			Mapper<Object, Text, MyKey, Text> {
		private MyKey tkey = new MyKey();
		private Text tvalue = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] logs = value.toString().split(" ");
			if (logs[0].matches("([a-z]+)")) {
				tkey.set(new Text(logs[0]));
				tvalue.set(logs[1]);
			} else {
				tkey.set(new Text(logs[1]));
				tvalue.set(logs[0]);
			}
			context.write(tkey, tvalue);
		}
	}

	public static class MyKey implements WritableComparable<MyKey> {
		private Text tkey = null;

		public MyKey() {
			tkey = new Text();
		}

		public Text get() {
			return this.tkey;
		}

		public void set(Text key) {
			this.tkey = key;
		}

		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			tkey.readFields(arg0);
		}

		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			tkey.write(arg0);
		}

		public int compareTo(MyKey o) {
			// TODO Auto-generated method stub
			return -tkey.compareTo(o.get());
		}

	}

	public static class KeyValueReducer extends
			Reducer<MyKey, Text, Text, Text> {
		// private Text tkey = new Text();

		public void reduce(MyKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, key.get());
			}
		}
	}

	public static class OutMapper extends Mapper<Text, Text, Text, Text> {

		protected void map(Text key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, value);
		}
	}

	public static class OutReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Text tkey = null;
			boolean flag = false;
			for (Text value : values) {
				if (!flag) {
					tkey = value;
					flag = true;
				} else
					context.write(new Text(tkey.toString()+"-"+key.toString()), value);
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: App <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Sorted Key Value 1");
		job.setJarByClass(App.class);
		job.setMapperClass(KeyValueMapper.class);
		job.setReducerClass(KeyValueReducer.class);
		job.setMapOutputKeyClass(MyKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputDirRecursive(job, true);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		Path tempPath = new Path("SortedKeyValue-temp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		FileOutputFormat.setOutputPath(job, tempPath);
		try {
			if (job.waitForCompletion(true)) {
				Job job2 = Job.getInstance(conf, "Sorted Key Value 2");
				job2.setJarByClass(App.class);
				job2.setMapperClass(OutMapper.class);
				job2.setReducerClass(OutReducer.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				job2.setInputFormatClass(SequenceFileInputFormat.class);
				FileInputFormat.addInputPath(job2, tempPath);
				FileOutputFormat.setOutputPath(job2, new Path(
						otherArgs[otherArgs.length - 1]));
				System.exit(job2.waitForCompletion(true) ? 0 : 1);
			}
		} finally {
			FileSystem.get(conf).deleteOnExit(tempPath);
		}
	}
}