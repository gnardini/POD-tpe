package combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query1CombinerFactory implements CombinerFactory<String, Integer, Integer> {

    @Override
    public Combiner<Integer, Integer> newCombiner(String key) {
        return new Query1Combiner();
    }

    private class Query1Combiner extends Combiner<Integer, Integer> {

        private int count;

        public Query1Combiner() {
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
