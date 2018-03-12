package io.datamass;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WritablePair implements Writable {

    public WritablePair(Integer k, Float v) {
        this.k = k;
        this.v = v;
    }

    private Integer k;
    private Float v;

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(k);
        dataOutput.writeFloat(v);
    }

    public void readFields(DataInput dataInput) throws IOException {
        k = dataInput.readInt();
        v = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return k + "\t" + v;
    }
}
