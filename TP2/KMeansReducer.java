package TP2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.shaded.com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KMeansReducer extends Reducer<BaryWritable, PointWritable, IntWritable, BaryWritable> {
    List<BaryWritable> barycenters = new ArrayList<>();

    @Override
    protected void reduce(BaryWritable actualBaryCenter, Iterable<PointWritable> clusterPoints, Reducer<BaryWritable, PointWritable, IntWritable, BaryWritable>.Context context) throws IOException, InterruptedException {
        super.reduce(actualBaryCenter, clusterPoints, context);
        for (PointWritable point : clusterPoints) {
            actualBaryCenter.add(point);
        }
        actualBaryCenter.divideBy(Iterators.size((Iterator<?>) clusterPoints));
        barycenters.add(actualBaryCenter);

        IntWritable outKey = new IntWritable(actualBaryCenter.getClusterId());
        BaryWritable outVal = actualBaryCenter;
        context.write(outKey, outVal);
    }

    @Override
    protected void cleanup(Reducer<BaryWritable, PointWritable, IntWritable, BaryWritable>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Configuration conf = context.getConfiguration();
        int taskId = context.getTaskAttemptID().getTaskID().getId();
        KMeans.recordBarycenters(conf, String.valueOf(taskId), barycenters);
    }
}
