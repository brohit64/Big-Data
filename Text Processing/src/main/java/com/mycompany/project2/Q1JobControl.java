/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.project2;
import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.io.IntWritable ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.mapreduce.Job ;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat ;
/**
 *
 * @author user1
 */
public class Q1JobControl {
public static void main ( String [] args ) throws Exception {
Configuration conf = new Configuration ();
Job job = Job . getInstance ( conf , " Q1 " );
job.setJarByClass(Q1JobControl.class );
job . setMapperClass ( Q1Mapper.class );
job . setCombinerClass ( Q1Reducer.class );
job . setReducerClass ( Q1Reducer.class );
job . setOutputKeyClass ( Text.class );
job.setOutputValueClass( IntWritable . class );
FileInputFormat . addInputPath ( job , new Path ( args [0]));
FileOutputFormat . setOutputPath ( job , new Path ( args [1]));
System.exit( job.waitForCompletion( true ) ? 0 : 1);
}

    
}




