package reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query7bReducerFactory implements ReducerFactory<String, Integer, Integer> {

    @Override
    public Reducer<Integer, Integer> newReducer(String department) {
        return new Query7bReducer();
    }

    private class Query7bReducer extends Reducer<Integer, Integer> {

        private int count;

        public Query7bReducer() {
        }

        @Override
        public void reduce(Integer count) {
            this.count += count;
        }

        @Override
        public Integer finalizeReduce() {
            return count;
        }
    }
}
