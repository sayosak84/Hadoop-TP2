package TP2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PointWritable implements Writable {

    protected double[] coordinates;

    PointWritable() {
        super();
    }

    PointWritable(double[] coordinates) {
        this.coordinates = coordinates;
    }

    PointWritable(String string) {
        // depend des coordinates
        String[] coordSplited = string.split(",");
        int size = coordSplited.length;
        this.coordinates = new double[size];
        for (int i = 0; i < size; i++) {
            this.coordinates[i] = Double.parseDouble(coordSplited[i]);
        }

    }

    public void write(DataOutput dout) throws IOException {
        int size = this.coordinates.length;
        dout.writeDouble(size);
        for (int i = 0; i < size; i++) {
            dout.writeDouble(coordinates[i]);
        }
    }

    public void readFields(DataInput din) throws IOException {
        double size = din.readDouble();
        for (int i = 0; i < size; i++) {
            this.coordinates[i] = din.readDouble();
        }
    }

    public static PointWritable read(DataInput in) throws IOException {
        PointWritable w = new PointWritable();
        w.readFields(in);
        return w;
    }

    public double[] getCoordinates() {
        return this.coordinates;
    }

    public String toString() {
        String toReturn = "";
        int size = this.coordinates.length;
        for (int i = 0; i < size; i++) {
            if (i == size - 1) {
                toReturn += String.valueOf(this.coordinates[i]);
            } else {
                toReturn += String.valueOf(this.coordinates[i]) + ",";
            }
        }
        return toReturn;
    }

    public double computeDistance(PointWritable point) {
        double distance = 0;
        int size = this.coordinates.length;
        double[] pointCoords = point.getCoordinates();

        for (int i = 0; i < size; i++) {
            distance += Math.pow((this.coordinates[i] - pointCoords[i]), 2);
        }

        return Math.sqrt(distance);
    }


}