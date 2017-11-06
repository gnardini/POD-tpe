package reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query4bReducerFactory implements ReducerFactory<String, Integer, Integer> {

    @Override
    public Reducer<Integer, Integer> newReducer(String s) { return new Query4bReducer(); }

    private class Query4bReducer extends Reducer<Integer, Integer> {

        private Integer count;

        public Query4bReducer() {
            count = 0;
        }

        @Override
        public void reduce(Integer c) {
            count += c;
        }

        @Override
        public Integer finalizeReduce() {
            return count;
        }

    }

}
