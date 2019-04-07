/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.project2;

import java.io.IOException ;
import org.apache.hadoop.io.IntWritable ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.mapreduce.Reducer ;

/**
 *
 * @author user1
 */
public class Q3Reducer extends Reducer <Text ,IntWritable ,Text , IntWritable > {
    
  enum Records
    {
        WORDS,
        PLURAL,
        SINGULAR;
        
    }
private IntWritable result = new IntWritable ();
public void reduce ( Text key , Iterable < IntWritable > values ,Context context ) throws IOException , InterruptedException
{
int sum = 0;
for ( IntWritable val : values ) 
{
sum+= val.get();
}
String keystr = key.toString();
if(keystr.equals("me") || keystr.equals("my") || keystr.equals("mine") || keystr.equals("i"))
    context.getCounter(Records.SINGULAR).increment(sum);
if(keystr.equals("us") || keystr.equals("our") || keystr.equals("ours") || keystr.equals("we"))
    context.getCounter(Records.PLURAL).increment(sum);
if(sum<4)
{
context.getCounter(Records.WORDS).increment(1);
}
result.set(sum);
context.write(key ,result );
}
}


