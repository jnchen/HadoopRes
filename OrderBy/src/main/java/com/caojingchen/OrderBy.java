package com.caojingchen;

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



public class OrderBy 
{
	public static class MyMapper extends Mapper<Object,Text,MyKey,Text>{
		private MyKey k= new MyKey();
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, MyKey, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line[] = value.toString().split("\t");
			if(line.length<2)return;
			k.setK(new Text(line[0]));
			k.setV(new Text(line[1]));
			context.write(k,new Text(line[1]));
		}
		
	}
	public static class MyReducer extends Reducer<MyKey,Text,Text,Text>{

		@Override
		protected void reduce(MyKey arg0, Iterable<Text> arg1,
				Reducer<MyKey, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Text value:arg1){
				arg2.write(arg0.getK(),value);
			}
		}
		
	}
	public static class MyGroupingComparator extends WritableComparator{
		public MyGroupingComparator(){
			super(MyKey.class,true);
		}
		@SuppressWarnings({ "rawtypes"})
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			MyKey o1 = (MyKey)a;
			MyKey o2 = (MyKey)b;
			return o1.getK().compareTo(o2.getK());
		}
	}

	public static class MyPartitioner extends Partitioner<MyKey,Text>{

		@Override
		public int getPartition(MyKey arg0, Text arg1, int arg2) {
			// TODO Auto-generated method stub
			return arg0.getK().hashCode()%arg2;
		}
	}
	
	
	public static class MyKey implements WritableComparable<MyKey>{
		private Text k = null;
		private Text v = null;
		public MyKey(){
			k = new Text();
			v = new Text();
		}
		public void setK(Text k){
			this.k = k;
		}
		public void setV(Text v){
			this.v = v;
		}
		public Text getK(){
			return this.k;
		}
		public Text getV(){
			return this.v;
		}
		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			k.readFields(arg0);
			v.readFields(arg0);
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			k.write(arg0);
			v.write(arg0);
		}

		@Override
		public int compareTo(MyKey arg0) {
			// TODO Auto-generated method stub
			if(k.compareTo(arg0.getK())==0){
				return v.compareTo(arg0.getV());
			}else{
				return -k.compareTo(arg0.getK());
			}
		}
		
	}
    public static void main( String[] args ) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
    {
    	Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err
					.println("Usage: com.caojingchen.OrderBy <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "OrderBy");
		job.setJarByClass(OrderBy.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(MyKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(MyPartitioner.class);
	
		job.setGroupingComparatorClass(MyGroupingComparator.class);

		FileInputFormat.setInputDirRecursive(job, true);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
