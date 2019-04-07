/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.pagerank;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.lang.Math;

/**
 *
 * @author user1
 */
public class PRReducer extends Reducer<LongWritable,Text,LongWritable,Text> {
    public static enum Records
    {
        updates;
    }
public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    double probab=0;
    String Nodes="";
    double prevprobab=0;
    for (Text value: values)
    {
        String list[] = value.toString().split(" ");
        if(list[0].equalsIgnoreCase("value"))
        { 
           probab = probab+Double.parseDouble(list[1]);
        }
        else
        {
             prevprobab=Double.parseDouble(list[0]);        
             Nodes = list[2];
             
        }
            
    }
               if(Math.abs(prevprobab-probab)<.01)
               context.getCounter(Records.updates).increment(1);
               


        Text word = new Text(probab+" "+Nodes);
        context.write(key,word);    
}
    
}

