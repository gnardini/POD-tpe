package reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query1ReducerFactory implements ReducerFactory<String, Integer, Integer> {

    @Override
    public Reducer<Integer, Integer> newReducer(String region) {
        return new Query1Reducer();
    }

    private class Query1Reducer extends Reducer<Integer, Integer> {

        private int count;

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
