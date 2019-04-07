package com.mycompany.bfs;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 *
 * @author user1
 */
public class BFSMapper extends Mapper <Object, Text, LongWritable, Text>  {
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
        String line[] = value.toString().split(" ");
        int weight = Integer.parseInt(line[1]);
        int distance = weight<100?weight+1:weight;
        for(String node: line[2].split(","))
        {
         context.write(new LongWritable(Integer.parseInt(node)),new Text("value "+distance));   
        }
        
        context.write(new LongWritable(Integer.parseInt(line[0])),new Text("value "+line[1]));   
        context.write(new LongWritable(Integer.parseInt(line[0])),new Text("nodes "+line[2]));   
    }
    }
    
