package com.caojingchen;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;

import com.hadoop.compression.lzo.LzopCodec;
public class Upload 
{
    public static void main( String[] args ) throws IOException
    {
    	if(args.length<2){
    		System.out.println("Usage : Upload <localfile> <hdfsfile>");
    	}
        LzopCodec lc = new LzopCodec();
        GzipCodec gc = new GzipCodec();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path oPath = new Path("hdfs://computer1:8020"+args[1]);
        lc.setConf(conf);
        gc.setConf(conf);
        InputStream is = gc.createInputStream(new FileInputStream(args[0]));
        OutputStream os = lc.createOutputStream(fs.create(oPath));
      	byte[] input = new byte[1024];
      	while(is.read(input)>0){
      		os.write(input);
      	}
      	os.close();
      	is.close();
    }
}
