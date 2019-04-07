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
public class Q6Mapper extends Mapper < Object ,Text , Text , IntWritable > {
    
     HashSet<String> cache;
        public void setup( Context context)
    {
        cache = new HashSet<String>();
        String words="the an a i me my myself we our ours ourselves you your yours yourself yourselves he him his himself she her hers herself it its itself they them their theirs themselves what which who whom this that these those am is are was were be been being have has had having do does did doing a an the and but if or because as until while of at by for with about against between into through during before after above below to from up down in out on off over under again further then once here there when where why how all any both each few more most other some such no nor not only own same so tha too very s t can will just don should now d ll m o re ve y aint arent couldnt didnt doesnt hadnt hasnt havent isnt mightnt mustnt neednt shant shouldnt wasnt werent wont wouldnt";
        for(String word: words.split(" ") ) 
        cache.add(word);
    }
    
private final static IntWritable one = new IntWritable (1);
private Text word = new Text ();
public void map ( Object key , Text value ,Context context ) throws IOException , InterruptedException {
String filtervalue = value.toString().replaceAll("[^a-zA-Z0-9\\s]",""); 
filtervalue = filtervalue.toLowerCase(); //change 2
StringTokenizer itr = new StringTokenizer ( filtervalue );
while ( itr.hasMoreTokens ()) 
{
String temp = itr.nextToken ();

if(!temp.contains("filename") && temp.equals("justice"))
{
    if(itr.hasMoreTokens())
    { String nextword = itr.nextToken();
        if(!cache.contains(nextword))
        {
        word.set(nextword);
        context.write(word,one );
        }
    }
}
}
}
}










/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */



