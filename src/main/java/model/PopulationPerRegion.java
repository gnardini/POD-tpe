package model;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class PopulationPerRegion implements DataSerializable {

	private String region;
	private int population;
	
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
	
	public PopulationPerRegion addPopulation(String region, int population){
		if(!this.region.equals(region)){
			throw new IllegalArgumentException();
		}
		return new PopulationPerRegion(region, population+this.population);
	}
	
	public PopulationPerRegion addPopulation(PopulationPerRegion ppr){
		if(!ppr.region.equals(region)){
			throw new IllegalArgumentException();
		}
		return new PopulationPerRegion(region, population+ppr.population); 
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
