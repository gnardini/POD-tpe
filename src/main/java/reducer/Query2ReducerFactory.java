package reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import model.PoblatedDepartment;

public class Query2ReducerFactory implements ReducerFactory<String, Long, PoblatedDepartment> {

    @Override
    public Reducer<Long, PoblatedDepartment> newReducer(String department) {
        return new Query2Reducer(department);
    }

    private class Query2Reducer extends Reducer<Long, PoblatedDepartment> {

        private final String department;
        private long total;

        public Query2Reducer(String department) {
            this.department = department;
        }

        @Override
        public void reduce(Long value) {
            total += value;
        }

        @Override
        public PoblatedDepartment finalizeReduce() {
            return new PoblatedDepartment(department, total);
        }
    }

}
