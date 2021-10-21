package TP2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.WordCount;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class KMeans
{

    public static String PROP_BARY_PATH;
    public static int TER_MAX = 1;

	public static List<BaryWritable> readBarycenters (Configuration config, String filename){
		List<BaryWritable> listBarycenters = new ArrayList<>();
		Path path = new Path(config.get(PROP_BARY_PATH)+"/"+filename);
		SequenceFile.Reader.Option seqInput = SequenceFile.Reader.file(path);
		try {
			SequenceFile.Reader reader = new SequenceFile.Reader(config, seqInput);
			IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), config);
			BaryWritable baryWritableValue = (BaryWritable) ReflectionUtils.newInstance(reader.getValueClass(), config);
			while (reader.next(key, baryWritableValue)){
				listBarycenters.add(baryWritableValue);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return listBarycenters;
	}

	public void recordBarycenters(Configuration config, String filename, List<BaryWritable> barycenters){
		Path path = new Path(config.get(PROP_BARY_PATH)+"/"+filename);
		try {
			FileSystem fs = FileSystem.get(config);
			if(fs.exists(path)) fs.delete(path, true);
			SequenceFile.Writer out = SequenceFile.createWriter(config, SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(BaryWritable.class));
			for (BaryWritable bary : barycenters){
				out.append(bary.getClusterId(),bary);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception{
		/*Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Custom Word Count Program");

		// on définie nos classes
		job.setJarByClass(WordCount.class);
		//job.setMapperClass(WordCountMapper.class);
		//job.setReducerClass(WordCountReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(BaryWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);*/
		KMeansMapper k = new KMeansMapper();
	}



}