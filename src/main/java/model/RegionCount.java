package model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class RegionCount implements DataSerializable {

    private String region;
    private int count;

    public RegionCount() {
    }

    public RegionCount(String region, int count) {
        this.region = region;
        this.count = count;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(region);
        out.writeInt(count);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        region = in.readUTF();
        count = in.readInt();
    }

    public String getRegion() {
        return region;
    }

    public int getCount() {
        return count;
    }
}
