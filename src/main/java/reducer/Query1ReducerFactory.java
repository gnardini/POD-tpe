package reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import model.RegionCount;

public class Query1ReducerFactory implements ReducerFactory<String, Integer, RegionCount> {

    @Override
    public Reducer<Integer, RegionCount> newReducer(String region) {
        return new Query1Reducer(region);
    }

    private class Query1Reducer extends Reducer<Integer, RegionCount> {

        private final String region;
        private int count;

        public Query1Reducer(String region) {
            this.region = region;
            count = 0;
        }

        @Override
        public void reduce(Integer count) {
            this.count += count;
        }

        @Override
        public RegionCount finalizeReduce() {
            return new RegionCount(region, count);
        }
    }

}
