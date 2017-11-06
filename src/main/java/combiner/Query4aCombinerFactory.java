package combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query4aCombinerFactory implements CombinerFactory<Integer, String, String> {

    @Override
    public Combiner<String, String> newCombiner(Integer key) {
        return new Query4aCombinerFactory.Query4aCombiner();
    }

    private class Query4aCombiner extends Combiner<String, String> {

        private String region;

        @Override
        public void combine(String r) {
            region = r;
        }

        @Override
        public String finalizeChunk() {
            return region;
        }

        @Override
        public void reset() {
            region = null;
        }
    }
}
