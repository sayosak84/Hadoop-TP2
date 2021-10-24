package TP2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, BaryWritable> {
    private List<BaryWritable> barycenters = new ArrayList<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, IntWritable, BaryWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        barycenters = KMeans.readBarycenters(conf, "all");     // recuperer les barycentres
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, BaryWritable>.Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        String[] colones = value.toString().split(",");
        if (!"attr".equals(colones[0])){                // eviter la premiere ligne des attributs
            PointWritable point = new PointWritable(value.toString());

            // on cherche le barycentre le plus proche du point
            double lowDistance = Double.MAX_VALUE;
            BaryWritable closestBarycenter = barycenters.get(0);
            for (BaryWritable barycenter : barycenters) {
                double distance = barycenter.computeDistance(point);
                if (lowDistance > distance) {
                    lowDistance = distance;
                    closestBarycenter = barycenter;
                }
            }

            // en sortie le clusterID et le barycentre le plus proche
            IntWritable outKey = new IntWritable(closestBarycenter.getClusterId());
            BaryWritable outVal = closestBarycenter;
            context.write(outKey, outVal);
        }
    }
}
