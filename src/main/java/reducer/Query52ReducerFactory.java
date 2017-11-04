package reducer;

import model.PopulationPerRegion;
import model.RegionInfo;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query52ReducerFactory implements ReducerFactory<String, RegionInfo, Double>{

    @Override
    public Reducer<RegionInfo, Double> newReducer(String region) {
        return new Query52Reducer();
    }

    private class Query52Reducer extends Reducer<RegionInfo, Double> {

    	public RegionInfo regionInfo;
    	
    	public Query52Reducer(){
    		regionInfo = new RegionInfo(0,0);
    	}
    	
        @Override
        public void reduce(RegionInfo region) {
        	regionInfo.add(region);
        }

        @Override
        public Double finalizeReduce() {
            return regionInfo.getPopulation() / (double)regionInfo.getHomes();
        }
    }

}
