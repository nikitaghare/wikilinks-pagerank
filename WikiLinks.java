package PageRank;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.util.Scanner;
import java.text.DecimalFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class WikiLinks {

    public static long count = 0;

    public static void main(String[] args) throws Exception {
        WikiLinks mainObject = new WikiLinks();

        String input=args[0];
        String output=args[1];


        int noOfIterations = 8;
        String Bucket = output + "/graph/";
        String tmpLoc = output + "/temp/";


        String[] iterations = new String[noOfIterations+1];

        for ( int i =0; i <= noOfIterations ; i++){
            iterations[i] = "WikiLinks.iter" + Integer.toString(i) +".out";
        }

        mainObject.Job1(input, tmpLoc);
        mainObject.Job2(tmpLoc, Bucket );

    }

    public void Job1(String input, String output) throws IOException {
        JobConf conf = new JobConf(WikiLinks.class);

        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
        conf.setJarByClass(WikiLinks.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setInputFormat(XmlInputFormat.class);
        conf.setMapperClass(MapperJob1.class);

        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(ReducerJob1.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        JobClient.runJob(conf);
    }

    public void Job2(String input, String output) throws IOException {
        JobConf conf = new JobConf(WikiLinks.class);
        conf.setJarByClass(WikiLinks.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setMapperClass(MapperJob2.class);

        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(ReducerJob2.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        JobClient.runJob(conf);
    }
  }