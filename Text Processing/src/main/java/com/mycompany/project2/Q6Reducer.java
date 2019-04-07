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
public class Q6Reducer extends Reducer <Text ,IntWritable ,IntWritable , Text > {
private IntWritable result = new IntWritable ();
public void reduce ( Text key , Iterable < IntWritable > values ,Context context ) throws IOException , InterruptedException{
int sum = 0;
for ( IntWritable val : values ) {
sum += val . get ();
}
result . set ( sum );
context . write ( result , key );
}
}


