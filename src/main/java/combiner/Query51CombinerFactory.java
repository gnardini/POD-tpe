package combiner;

import model.PopulationPerRegion;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query51CombinerFactory implements CombinerFactory<Integer, String, PopulationPerRegion> {

    @Override
    public Combiner<String, PopulationPerRegion> newCombiner(Integer homeId) {
        return new Query51Combiner();
    }

    private class Query51Combiner extends Combiner<String, PopulationPerRegion> {

        private PopulationPerRegion region = null;

        @Override
        public void combine(String regionString) {
            if(region == null){
            	region = new PopulationPerRegion(regionString, 1);
            }else{
            	region.addPopulation(regionString, 1);
            }
        }

        @Override
        public void reset() {
            region = null;
        }

        @Override
        public PopulationPerRegion finalizeChunk() {
            return region;
        }
    }
}
