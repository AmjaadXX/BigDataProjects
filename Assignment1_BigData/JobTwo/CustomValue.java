import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CustomValue implements Writable {

    private float sum ;
    private int count = 1;

    public float getSum() {
        return sum;
    }
    public void setSum(float sum) {
        this.sum = sum;
    }
    public int getCount() {
        return count;
    }
    public void setCount(int count) {
        this.count = count;
    }
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        sum = in.readFloat();
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeFloat(sum);
    }
    public String toString() {
        return count + " , " + sum;
    }
}
