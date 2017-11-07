package combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import model.RegionInfo;

public class Query5CombinerFactory implements CombinerFactory<String, Integer, RegionInfo> {

    @Override
    public Combiner<Integer, RegionInfo> newCombiner(String region) {
        return new Query5Combiner();
    }

    private class Query5Combiner extends Combiner<Integer, RegionInfo> {

        private RegionInfo regionInfo = new RegionInfo();

        @Override
        public void combine(Integer homeId) {
            regionInfo.addHomeId(homeId);
        }

        @Override
        public void reset() {
            regionInfo = new RegionInfo();
        }

        @Override
        public RegionInfo finalizeChunk() {
            return regionInfo;
        }
    }
}
