package TP2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansReducer extends Reducer<IntWritable, BaryWritable, IntWritable, BaryWritable> {
    List<BaryWritable> barycenters = new ArrayList<>();

    @Override
    protected void reduce(IntWritable key, Iterable<BaryWritable> values, Reducer<IntWritable, BaryWritable, IntWritable, BaryWritable>.Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);

    }
}
