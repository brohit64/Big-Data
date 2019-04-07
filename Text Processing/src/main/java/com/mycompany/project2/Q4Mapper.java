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
/**
 *
 * @author user1
 */
public class Q4Mapper extends Mapper < Object ,Text , Text , Text > {
    
private Text word = new Text ();
private Text file = new Text ();
public void map ( Object key , Text value ,Context context ) throws IOException , InterruptedException {
String filtervalue = value.toString().replaceAll("[^a-zA-Z0-9\\s]",""); //change 1
filtervalue = filtervalue.toLowerCase(); //change 2
String[] itr =  filtervalue.split(" ");
String filename = itr[itr.length-1].substring(8,itr[itr.length-1].length());
file.set(filename);
for(String val: itr) 
{
if(!val.contains("filename"))   //change 3,4,5
{
word.set(val);
context.write(word,file );
}
}
}
}




/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


