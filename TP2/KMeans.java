package TP2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.WordCount;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.ArrayList;
import java.util.List;


public class KMeans
{

    public static String PROP_BARY_PATH;
    public static int TER_MAX = 1;

	public static List<BaryWritable> readBarycenters (Configuration config, String filename){
		List<BaryWritable> listBarycenters = new ArrayList<>();
		Path path = new Path(config.get(PROP_BARY_PATH)+"/"+filename);



		return listBarycenters;
	}

	public static void main(String[] args) throws Exception{
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Custom Word Count Program");

		// on définie nos classes
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}



}