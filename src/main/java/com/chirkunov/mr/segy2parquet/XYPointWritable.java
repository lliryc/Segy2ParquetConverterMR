package com.chirkunov.mr.segy2parquet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class XYPointWritable implements WritableComparable {

    public XYPointWritable(int x, int y){
        this.x = x;
        this.y = y;
    }

    public XYPointWritable(){
    }

    private int x;
    public int getX(){
        return x;
    }

    private int y;
    public int getY(){
        return y;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(x);
        dataOutput.writeInt(y);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        x = dataInput.readInt();
        y = dataInput.readInt();
    }

    @Override
    public String toString() {
        return String.format("(%d, %d)",x, y);
    }

    @Override
    public int compareTo(Object o) {
        XYPointWritable pt = (XYPointWritable)o;
        if (x != pt.x){
            return x - pt.x;
        }
        return y - pt.y;
    }
}
