package com.caojingchen.SortedKeyValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortedKV {
	public static class KeyValueMapper extends
			Mapper<Object, Text, MyKey, Text> {
		private MyKey tkey = new MyKey();
		private Text tvalue = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] logs = value.toString().split(" ");
			if (logs[0].matches("([a-z]+)")) {
				tkey.setKey(new Text(logs[1]));
				tkey.setValue(new Text(logs[0]));
				tvalue.set(logs[0]);
			} else {
				tkey.setKey(new Text(logs[0]));
				tkey.setValue(new Text(logs[1]));
				tvalue.set(logs[1]);
			}
			context.write(tkey, tvalue);
		}
	}

	public static class MyValueComparator extends WritableComparator {
		public MyValueComparator() {
			super(MyKey.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (!((MyKey) a).getKey().toString().equals(((MyKey) b).getKey().toString())){
				return ((MyKey) a).getKey().compareTo(((MyKey) b).getKey());
			} else {
				Text v1 = ((MyKey) a).getValue();
				Text v2 = ((MyKey) b).getValue();

				if (Character.isDigit(v1.toString().charAt(0))
						&& Character.isDigit(v2.toString().charAt(0))) {
					return v1.compareTo(v2);
				} else if (Character.isLetter(v1.toString().charAt(0))
						&& Character.isLetter(v2.toString().charAt(0))) {
					return v1.compareTo(v2);
				} else if (Character.isDigit(v1.toString().charAt(0))
						&& Character.isLetter(v2.toString().charAt(0))) {
					return -v1.compareTo(v2);
				} else if (Character.isLetter(v1.toString().charAt(0))
						&& Character.isDigit(v2.toString().charAt(0))) {
					return -v1.compareTo(v2);
				} else {
					return -1;
				}
			}
		}
	}

	public static class MyComparator extends WritableComparator {
		public MyComparator() {
			super(MyKey.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			MyKey ma = (MyKey) a;
			MyKey mb = (MyKey) b;
			return ma.getKey().compareTo(mb.getKey());
		}

	}

	public static class MyPartitioner extends Partitioner<MyKey, Text> {

		@Override
		public int getPartition(MyKey key, Text value, int numPartitions) {
			// TODO Auto-generated method stub
			return key.getKey().hashCode() % numPartitions;
		}

	}

	public static class MyKey implements WritableComparable<MyKey> {
		private Text tkey = null;
		private Text tValue = null;

		public MyKey() {
			tkey = new Text();
			tValue = new Text();
		}

		public Text getKey() {
			return this.tkey;
		}

		public void setKey(Text key) {
			this.tkey = key;
		}

		public Text getValue() {
			return this.tValue;
		}

		public void setValue(Text value) {
			this.tValue = value;
		}

		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			tkey.readFields(arg0);
			tValue.readFields(arg0);
		}

		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			tkey.write(arg0);
			tValue.write(arg0);
		}

		public int compareTo(MyKey o) {
			// TODO Auto-generated method stub
			return tValue.compareTo(o.getValue());
			// return tkey.compareTo(o.getKey());
		}

	}

	public static class KeyValueReducer extends
			Reducer<MyKey, Text, Text, Text> {
		// private Text tkey = new Text();

		public void reduce(MyKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String newKey = null;
			int count = 0;
			for (Text value : values) {
				if (count == 0) {
					newKey = value.toString();
					count++;
				} else {
					context.write(new Text(newKey), key.getValue());
				}
				// context.write(key.getKey(),key.getValue());
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err
					.println("Usage: com.caojingchen.SortedKeyValue.SortedKV <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "SortedKV");
		job.setJarByClass(SortedKV.class);
		job.setMapperClass(KeyValueMapper.class);
		job.setReducerClass(KeyValueReducer.class);

		job.setMapOutputKeyClass(MyKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(MyPartitioner.class);
		job.setSortComparatorClass(MyValueComparator.class);
		job.setGroupingComparatorClass(MyComparator.class);

		FileInputFormat.setInputDirRecursive(job, true);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
