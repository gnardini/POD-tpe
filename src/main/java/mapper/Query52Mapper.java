package mapper;

import model.PopulationPerRegion;
import model.RegionInfo;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query52Mapper implements
		Mapper<Integer, PopulationPerRegion, String, RegionInfo> {

	@Override
	public void map(Integer homeId, PopulationPerRegion ppr,
			Context<String, RegionInfo> context) {
		context.emit(ppr.getRegion(), new RegionInfo(1, ppr.getPopulation()));
	}

}
