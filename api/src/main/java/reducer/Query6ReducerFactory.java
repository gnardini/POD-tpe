package reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class Query6ReducerFactory implements ReducerFactory<String, Set<String>, Integer> {

    @Override
    public Reducer<Set<String>, Integer> newReducer(String s) {
        return new Query6Reducer();
    }

    private class Query6Reducer extends Reducer<Set<String>, Integer> {

        private Set<String> provinces;

        public Query6Reducer() {
            provinces = new HashSet<>();
        }

        @Override
        public void reduce(Set<String> provinces) {
            this.provinces.addAll(provinces);
        }

        @Override
        public Integer finalizeReduce() {
            return provinces.size();
        }
    }
}
