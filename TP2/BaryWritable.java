package TP2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BaryWritable extends PointWritable implements WritableComparable {

    private int clusterId;

    public int getClusterId() {
        return clusterId;
    }

    public BaryWritable() {
        super();
    }

    public BaryWritable(double[] coordinates, int clusterId) {
        super(coordinates);
        this.clusterId = clusterId;
    }

    public void write(DataOutput dout) throws IOException {
        dout.writeDouble(clusterId);
        super.write(dout);
    }

    public void readFields(DataInput din) throws IOException {
        this.clusterId =(int) din.readDouble();
        super.readFields(din);
    }

    @Override
    public int compareTo(Object o) {
        BaryWritable bary = (BaryWritable) o;
        int baryClusterID = bary.getClusterId();
        if (this.clusterId > baryClusterID){
            return 1;
        }  else if (this.clusterId < baryClusterID){
            return -1;
        } else{
            return 0;
        }
    }

    public boolean equals(Object o) {
        if (compareTo(o) == 0){
            return true;
        }
        return false;
    }

    public String toString() {
        return "clusterID : " + String.valueOf(this.clusterId);
    }

    void add(PointWritable point){
        int coordSize = this.coordinates.length;
        for (int i = 0; i < coordSize; i++){
            coordinates[i] += point.coordinates[i] ;
        }
    }

    void divideBy(int size){
        int coordSize = this.coordinates.length;
        for (int i = 0; i < coordSize; i++){
            coordinates[i] = coordinates[i] / size;
        }
    }

}
