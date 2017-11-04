package reducer;

import model.PopulationPerRegion;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query51ReducerFactory implements ReducerFactory<Integer, PopulationPerRegion, PopulationPerRegion>{

    @Override
    public Reducer<PopulationPerRegion, PopulationPerRegion> newReducer(Integer key) {
        return new Query51Reducer();
    }

    private class Query51Reducer extends Reducer<PopulationPerRegion, PopulationPerRegion> {

        private PopulationPerRegion region = null;

        @Override
        public void reduce(PopulationPerRegion region) {
        	if(region == null){
            	this.region = region;
            }else{
            	this.region.addPopulation(region);
            }
        }

        @Override
        public PopulationPerRegion finalizeReduce() {
            return region;
        }
    }

}
