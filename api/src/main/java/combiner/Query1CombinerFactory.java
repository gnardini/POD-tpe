package combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query1CombinerFactory implements CombinerFactory<String, Integer, Integer> {

    @Override
    public Combiner<Integer, Integer> newCombiner(String key) {
        return new Query1Combiner(key);
    }

    private class Query1Combiner extends Combiner<Integer, Integer> {

        private final String region;
        private int count;

        public Query1Combiner(String region) {
            this.region = region;
            count = 0;
        }

        @Override
        public void combine(Integer count) {
            this.count += count;
        }

        @Override
        public void reset() {
            count = 0;
        }

        @Override
        public Integer finalizeChunk() {
            return count;
        }
    }

}
