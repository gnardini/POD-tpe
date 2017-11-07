package model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Basic information about the population of a region.
 */
public class RegionPopulation implements DataSerializable {

    private String region;
    private long population;

    public RegionPopulation() {
    }

    public RegionPopulation(String region, long population) {
        this.region = region;
        this.population = population;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(region);
        out.writeLong(population);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        region = in.readUTF();
        population = in.readLong();
    }

    public String getRegion() {
        return region;
    }

    public long getPopulation() {
        return population;
    }
}
