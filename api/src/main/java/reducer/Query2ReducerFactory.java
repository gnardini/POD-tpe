package reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import model.DepartmentPopulation;

public class Query2ReducerFactory implements ReducerFactory<String, Long, DepartmentPopulation> {

    @Override
    public Reducer<Long, DepartmentPopulation> newReducer(String department) {
        return new Query2Reducer(department);
    }

    private class Query2Reducer extends Reducer<Long, DepartmentPopulation> {

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
        public DepartmentPopulation finalizeReduce() {
            return new DepartmentPopulation(department, total);
        }
    }

}
