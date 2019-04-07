/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.project2;

import java.io.IOException ;
import java.util.HashSet;
import java.util.StringTokenizer ;
import org.apache.hadoop.io.IntWritable ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.mapreduce.Mapper ;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Collection;


/**
 *
 * @author user1
 */
public class Q2Mapper extends Mapper < Object ,Text , Text , IntWritable > {
    HashSet<String> cache;
        public void setup( Context context)
    {
        cache = new HashSet<String>();
        String words="the an a";
        for(String word: words.split(" ") ) 
        cache.add(word);
    }
    
private final static IntWritable one = new IntWritable (1);
private Text word = new Text ();
public void map ( Object key , Text value ,Context context ) throws IOException , InterruptedException {
String filtervalue = value.toString().replaceAll("[^a-zA-Z0-9\\s]",""); //change 1
filtervalue = filtervalue.toLowerCase(); //change 2
StringTokenizer itr = new StringTokenizer ( filtervalue );
while ( itr.hasMoreTokens ()) 
{
String temp = itr.nextToken ();
if(!cache.contains(temp) && !temp.contains("anti") && !temp.endsWith("tion") && !temp.contains("filename"))   //change 3,4,5
{
word.set(temp);
context.write(word,one );
}
}
}
}

