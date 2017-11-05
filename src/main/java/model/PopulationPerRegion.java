package model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class PopulationPerRegion implements DataSerializable {

	private String region;
	private int population;

	public PopulationPerRegion() {
	}

	public PopulationPerRegion(String region, int population) {
		this.region = region;
		this.population = population;
	}
	
	public int getPopulation() {
		return population;
	}
	
	public String getRegion() {
		return region;
	}
	
	public void addPopulation(String region, int population) {
		if(!this.region.equals(region)){
			throw new IllegalArgumentException();
		}
		this.population += population;
	}
	
	public void addPopulation(PopulationPerRegion ppr){
		if(!ppr.region.equals(region)){
			throw new IllegalArgumentException();
		}
		population += ppr.population;
	}
	
	@Override
	public void readData(ObjectDataInput in) throws IOException {
		region = in.readUTF();
		population = in.readInt();
	}
	
	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(region);
		out.writeInt(population);
	}
}
