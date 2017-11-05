package combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query7bCombinerFactory implements CombinerFactory<String, Integer, Integer> {

    @Override
    public Combiner<Integer, Integer> newCombiner(String key) {
        return new Query7bCombiner();
    }

    private class Query7bCombiner extends Combiner<Integer, Integer> {

        private int count;

        public Query7bCombiner() {
            count = 0;
        }

        @Override
        public void combine(Integer count) {
            this.count += count;
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


