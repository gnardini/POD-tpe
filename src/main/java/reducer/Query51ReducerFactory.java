package reducer;

import model.PopulationPerRegion;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query51ReducerFactory implements ReducerFactory<Integer, PopulationPerRegion, PopulationPerRegion> {

    @Override
    public Reducer<PopulationPerRegion, PopulationPerRegion> newReducer(Integer key) {
        return new Query51Reducer();
    }

    private class Query51Reducer extends Reducer<PopulationPerRegion, PopulationPerRegion> {

        private String regionString;
        private int count = 0;

        @Override
        public void reduce(PopulationPerRegion region) {
            this.regionString = region.getRegion();
            this.count += region.getPopulation();
        }

        @Override
        public PopulationPerRegion finalizeReduce() {
            return new PopulationPerRegion(regionString, count);
        }
    }

}
