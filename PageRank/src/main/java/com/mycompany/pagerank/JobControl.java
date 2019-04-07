/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author user1
 */
public class JobControl {

public static void main(String[] args) throws Exception {

long count = 0;
    while(count!=5)
    {
    count = 0;
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", " ");
    Job job = Job.getInstance(conf, " PR ");
    job.setJarByClass(JobControl.class);
    job.setMapperClass(PRMapper.class);
    job.setReducerClass(PRReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    Path source = new Path(args [0]);
    Path destination = new Path(args [1]);
    FileInputFormat.addInputPath(job, source);
    FileOutputFormat.setOutputPath(job, destination);
    job.waitForCompletion(true);
    Counters cn = job.getCounters();
    Counter updates = cn.findCounter(PRReducer.Records.updates);
    count = updates.getValue(); 
    if(count!=5)
    {
         
    Path source_file = new Path("/pagerank");
    Path target_file = new Path("/result/pagerank/part-r-00000");
    FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path("/pagerank/part-r-00000"),true);
    fs.rename(target_file, source_file);    
    fs.delete(destination,true);
    }
    }
}

}




