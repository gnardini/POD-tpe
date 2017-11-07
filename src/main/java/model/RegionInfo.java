package model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class RegionInfo implements DataSerializable {

	private Set<Integer> homeIds;
	private int population;

	public RegionInfo() {
		homeIds = new HashSet<>();
	}

	public void addHomeId(int homeId) {
	    homeIds.add(homeId);
	    population++;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(population);
        out.writeInt(homeIds.size());
        for (Integer id: homeIds) {
            out.writeInt(id);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        population = in.readInt();
        homeIds = new HashSet<>();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            homeIds.add(in.readInt());
        }
    }

    public int getPopulation() {
        return population;
    }

    public Set<Integer> getHomeIds() {
        return homeIds;
    }

    public double getPeoplePerHome() {
        return population / (double) homeIds.size();
    }

    public void addRegionInfo(RegionInfo region) {
        population += region.getPopulation();
        homeIds.addAll(region.getHomeIds());
    }
}
