package reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import model.EmploymentData;

public class Query4aReducerFactory implements ReducerFactory<Integer, String, String> {

    @Override
    public Reducer<String, String> newReducer(Integer k) { return new Query4aReducer(); }

    private class Query4aReducer extends Reducer<String, String> {

        private String region;

        @Override
        public void reduce(String r) {
            region = r;
        }

        @Override
        public String finalizeReduce() {
            return region;
        }
    }
}
