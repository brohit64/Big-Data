/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.project2;

import java.io.IOException ;
import java.util.StringTokenizer ;
import org.apache.hadoop.io.IntWritable ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.mapreduce.Mapper ;


/**
 *
 * @author user1
 */
public class Q1Mapper extends Mapper < Object ,
Text , Text , IntWritable > {
private final static IntWritable one = new IntWritable (1);
private Text word = new Text ();
public void map ( Object key , Text value ,Context context ) throws IOException , InterruptedException {
StringTokenizer itr = new StringTokenizer ( value . toString ());
while ( itr . hasMoreTokens ()) {
String temp = itr . nextToken (); 
if(!temp.contains("FILENAME"))
{ 
    word.set ( temp );
context . write ( word , one );
}
}
}
}

