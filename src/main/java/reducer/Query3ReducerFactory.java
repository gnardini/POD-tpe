package reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import model.EmploymentData;

public class Query3ReducerFactory implements ReducerFactory<String, EmploymentData, Double> {


    @Override
    public Reducer<EmploymentData, Double> newReducer(String s) { return new Query3Reducer(); }

    private class Query3Reducer extends Reducer<EmploymentData, Double> {

        private EmploymentData employmentData;

        public Query3Reducer() {
            employmentData = new EmploymentData(0,0);
        }

        @Override
        public void reduce(EmploymentData e) {
            employmentData.employedPeople += e.employedPeople;
            employmentData.unemployedPeople += e.unemployedPeople;
        }

        @Override
        public Double finalizeReduce() {
            return employmentData.unemployedPeople /
                    (double)(employmentData.employedPeople + employmentData.unemployedPeople);
        }
    }
}
