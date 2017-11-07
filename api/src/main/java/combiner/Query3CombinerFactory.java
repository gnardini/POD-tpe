package combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import model.EmploymentData;

public class Query3CombinerFactory implements CombinerFactory<String, EmploymentData, EmploymentData> {

    @Override
    public Combiner<EmploymentData, EmploymentData> newCombiner(String key) {
        return new Query3CombinerFactory.Query3Combiner();
    }

    private class Query3Combiner extends Combiner<EmploymentData, EmploymentData> {

        private EmploymentData employmentData;

        public Query3Combiner() {
            this.employmentData= new EmploymentData(0, 0);
        }

        @Override
        public void combine(EmploymentData e) {
            this.employmentData.employedPeople += e.employedPeople;
            this.employmentData.unemployedPeople += e.unemployedPeople;
        }

        @Override
        public EmploymentData finalizeChunk() {
            return employmentData;
        }

        @Override
        public void reset() {
            employmentData = new EmploymentData(0,0);
        }
    }
}
