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
import java.util.HashSet;
import java.util.Iterator;

/**
 *
 * @author user1
 */
public class Q4Reducer extends Reducer <Text ,Text ,Text ,Text > {
private IntWritable result = new IntWritable ();
public void reduce ( Text key , Iterable < Text > values ,Context context ) throws IOException , InterruptedException{
HashSet<Text> docs= new HashSet<Text>();
Text result = new Text();
for ( Text val : values ) {
docs.add(val);
}
if(docs.size()==1)
{
result=(Text)docs.toArray()[0];
context.write( key ,result);
}
}
}




/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

