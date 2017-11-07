package reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query2ReducerFactory implements ReducerFactory<String, Long, Long> {

    @Override
    public Reducer<Long, Long> newReducer(String department) {
        return new Query2Reducer();
    }

    private class Query2Reducer extends Reducer<Long, Long> {

        private long total;

        @Override
        public void reduce(Long value) {
            total += value;
        }

        @Override
        public Long finalizeReduce() {
            return total;
        }
    }

}
