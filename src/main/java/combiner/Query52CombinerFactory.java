package combiner;

import model.PopulationPerRegion;
import model.RegionInfo;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query52CombinerFactory implements CombinerFactory<String, RegionInfo, RegionInfo> {

    @Override
    public Combiner<RegionInfo, RegionInfo> newCombiner(String region) {
        return new Query52Combiner();
    }

    private class Query52Combiner extends Combiner<RegionInfo, RegionInfo> {

    	RegionInfo r;
    	
        public Query52Combiner() {
			r = new RegionInfo(0, 0);
		}

        @Override
        public void combine(RegionInfo regionInfo) {
            r.add(regionInfo);
        }

        @Override
        public void reset() {
            r = new RegionInfo(0, 0);
        }

        @Override
        public RegionInfo finalizeChunk() {
            return r;
        }
    }
}
