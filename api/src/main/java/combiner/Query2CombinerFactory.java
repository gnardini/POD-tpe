package combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query2CombinerFactory implements CombinerFactory<String, Long, Long> {

    @Override
    public Combiner<Long, Long> newCombiner(String key) {
        return new Query2Combiner();
    }

    private class Query2Combiner extends Combiner<Long, Long> {

        private long total;

        @Override
        public void combine(Long value) {
            total += value;
        }

        @Override
        public void reset() {
            total = 0;
        }

        @Override
        public Long finalizeChunk() {
            return total;
        }
    }

}
