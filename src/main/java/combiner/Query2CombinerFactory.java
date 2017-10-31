package combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import model.PoblatedDepartment;

public class Query2CombinerFactory implements CombinerFactory<String, PoblatedDepartment, PoblatedDepartment> {

    @Override
    public Combiner<PoblatedDepartment, PoblatedDepartment> newCombiner(String key) {
        return new Query2Combiner(key);
    }

    private class Query2Combiner extends Combiner<PoblatedDepartment, PoblatedDepartment> {

        private final PoblatedDepartment department;

        public Query2Combiner(String department) {
            this.department = new PoblatedDepartment(department, new Long(0));
        }

        @Override
        public void combine(PoblatedDepartment department) {
            this.department.addPoblation(department.getPoblation());
        }

        @Override
        public PoblatedDepartment finalizeChunk() {
            return department;
        }
    }

}
