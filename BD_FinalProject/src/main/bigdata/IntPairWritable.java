package bigdata;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPairWritable implements WritableComparable<IntPairWritable> {

    int u, v;

    public void set(int u, int v) {
        this.u = u;
        this.v = v;
    }

    @Override
    public String toString() {
        return u + "\t" + v;
    }

    public int compareTo(IntPairWritable o) {
        if (this.u != o.u) return Integer.compare(this.u, o.u);
        return Integer.compare(this.v, o.v);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(u);
        dataOutput.writeInt(v);
    }

    public void readFields(DataInput dataInput) throws IOException {
        u = dataInput.readInt();
        v = dataInput.readInt();
    }
}
