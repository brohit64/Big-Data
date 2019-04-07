/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.pagerank;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author user1
 */
public class PRMapper extends Mapper <Object, Text, LongWritable, Text>  {
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
        String line[] = value.toString().split(" ");
        double weight = Double.parseDouble(line[1]);
        String adjlist[] = line[2].split(",");
        int size = adjlist.length;
        double probab = weight/size;
        for(String node: adjlist)
        {
         context.write(new LongWritable(Integer.parseInt(node)),new Text("value "+probab));   
        }
        
        context.write(new LongWritable(Integer.parseInt(line[0])),new Text(line[1]+" nodes "+line[2]));   
    }
    }
    

