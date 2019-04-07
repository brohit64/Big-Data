package com.mycompany.bfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.fs.FileSystem;
import java.io.*;
        
public class JobController {

public static void main(String[] args) throws Exception {

long count = 0;
    while(count!=7)
    {
    count = 0;
    Configuration conf = new Configuration();
    //conf.set("mapreduce.output.textoutputformat.separator", " ");
    conf.set("mapred.textoutputformat.separator", " ");
    Job job = Job.getInstance(conf, " BFS ");
    job.setJarByClass(JobController.class);
    job.setMapperClass(BFSMapper.class);
    job.setReducerClass(BFSReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    Path source = new Path(args [0]);
    Path destination = new Path(args [1]);
    FileInputFormat.addInputPath(job, source);
    FileOutputFormat.setOutputPath(job, destination);
    job.waitForCompletion(true);
    Counters cn = job.getCounters();
    Counter updates = cn.findCounter(BFSReducer.Records.updates);
    count = updates.getValue(); 
    if(count!=7)
    {
    Path source_file = new Path("/bfs");
    Path target_file = new Path("/result/files/part-r-00000");
    FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path("/bfs/part-r-00000"),true);
    fs.rename(target_file, source_file);    
    fs.delete(destination,true);
    }
    }
}

}

