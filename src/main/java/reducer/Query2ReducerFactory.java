package reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import model.PoblatedDepartment;
import model.RegionCount;

public class Query2ReducerFactory implements ReducerFactory<String, PoblatedDepartment, PoblatedDepartment> {

    @Override
    public Reducer<PoblatedDepartment, PoblatedDepartment> newReducer(String department) {
        return new Query2Reducer(department);
    }

    private class Query2Reducer extends Reducer<PoblatedDepartment, PoblatedDepartment> {

        private PoblatedDepartment department;

        public Query2Reducer(String department) {
        }

        @Override
        public void reduce(PoblatedDepartment value) {
            if (department == null) {
                department = value;
                return;
            }
            department.addPoblation(department.getPoblation());
        }

        @Override
        public PoblatedDepartment finalizeReduce() {
            return department;
        }
    }

}
