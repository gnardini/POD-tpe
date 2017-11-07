package combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query4CombinerFactory implements CombinerFactory<String, Integer, Integer> {

    @Override
    public Combiner<Integer, Integer> newCombiner(String key) {
        return new Query4Combiner();
    }

    private class Query4Combiner extends Combiner<Integer, Integer> {

        private Integer count;

        public Query4Combiner() {
            count = 0;
        }

        @Override
        public void combine(Integer c) {
            count += c;
        }

        @Override
        public Integer finalizeChunk() {
            return count;
        }

        @Override
        public void reset() {
            count = 0;
        }
    }
}
