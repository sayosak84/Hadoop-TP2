package TP2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.shaded.org.jline.utils.InputStreamReader;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class KMeans {

    public static String PROP_BARY_PATH;
    public static int TER_MAX = 1;

    public static List<BaryWritable> readBarycenters(Configuration config, String filename) {
        List<BaryWritable> listBarycenters = new ArrayList<>();
        Path path = new Path(config.get(PROP_BARY_PATH) + "/" + filename);
        SequenceFile.Reader.Option seqInput = SequenceFile.Reader.file(path);
        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(config, seqInput);
            IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), config);
            BaryWritable baryWritableValue = (BaryWritable) ReflectionUtils.newInstance(reader.getValueClass(), config);
            while (reader.next(key, baryWritableValue)) {
                listBarycenters.add(baryWritableValue);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return listBarycenters;
    }

    public static void recordBarycenters(Configuration config, String filename, List<BaryWritable> barycenters) {
        Path path = new Path(config.get(PROP_BARY_PATH) + "/" + filename);
        try {
            FileSystem fs = FileSystem.get(config);
            if (fs.exists(path)) fs.delete(path, true);    // effacer le fichier s'il existe
            SequenceFile.Writer out = SequenceFile.createWriter(config, SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(BaryWritable.class));
            for (BaryWritable bary : barycenters) {
                out.append(bary.getClusterId(), bary);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void initBarycenters(String fin, Configuration config, int k) {
        List<BaryWritable> barycenters = new ArrayList<>();

        Path path = new Path(fin);
        try {
            FileSystem fs = FileSystem.get(config);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            for (int i = 0; i < k; i++) {
                String line = br.readLine();
                if (i == 0) {     // eviter la premiere ligne
                    continue;
                }

                PointWritable tmpPoint = new PointWritable(line);
                BaryWritable barycenter = new BaryWritable(tmpPoint.getCoordinates(),i);

                barycenters.add(barycenter);
            }

            recordBarycenters(config, "all", barycenters);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void updateBarycenters(Configuration config){
        try {
            Path path = new Path(config.get(PROP_BARY_PATH) + "/" );
            FileSystem fs = FileSystem.get(config);
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, false);

            while (files.hasNext()){
                Path filePath = files.next().getPath();
                String filename = filePath.getName();              // on recupere le nom du fichier seulement
                List<BaryWritable> baryCenters = readBarycenters(config,filename);
                fs.delete(filePath, true);
                recordBarycenters(config,"all",baryCenters);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Job setupJob(int iteration, String fin, String fout) throws Exception {
        Configuration config = new Configuration();

        Path baryPath = new Path("tmp/barycenters");
        config.set(PROP_BARY_PATH, baryPath.toString());

        Job job = Job.getInstance(config, "Job iteration : "+ String.valueOf(iteration));

        // on définie nos classes
        job.setJarByClass(KMeans.class);
        job.setJarByClass(BaryWritable.class);
        job.setJarByClass(PointWritable.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        // on définie les types de sortie
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BaryWritable.class);

        FileInputFormat.addInputPath(job, new Path(fin));
        FileOutputFormat.setOutputPath(job, new Path(fout));

        return job;
    }

    public static void main(String[] args) throws Exception {
    }


}