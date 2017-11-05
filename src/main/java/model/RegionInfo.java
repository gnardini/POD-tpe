package model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class RegionInfo implements DataSerializable {

	private int homes;
	private int population;

	public RegionInfo() {
	}

	public RegionInfo(int homes, int population) {
		this.homes = homes;
		this.population = population;
	}
	
	public int getHomes() {
		return homes;
	}
	
	public int getPopulation() {
		return population;
	}
	
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(homes);
        out.writeInt(population);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        homes = in.readInt();
        population = in.readInt();
    }

	public void add(RegionInfo regionInfo) {
		homes += regionInfo.homes;
		population += regionInfo.population;
	}
}
