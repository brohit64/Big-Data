/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.bfs;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;



/**
 *
 * @author user1
 */
public class BFSReducer extends Reducer<LongWritable,Text,LongWritable,Text> {
    public static enum Records
    {
        updates;
    }
public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    int mindistance=100;
    String Nodes="";
    for (Text value: values)
    {
        String list[] = value.toString().split(" ");
        if(list[0].equalsIgnoreCase("value"))
        { 
           int distance = Integer.parseInt(list[1]);
     
           if(distance<mindistance)
           {
               mindistance = distance;
           }
        }
        else
        {
             Nodes = list[1];
        }
            
    }
               if(mindistance!=100)
               context.getCounter(Records.updates).increment(1);
               

        Text word = new Text(mindistance+" "+Nodes);
        context.write(key,word);

    
}
    
}
