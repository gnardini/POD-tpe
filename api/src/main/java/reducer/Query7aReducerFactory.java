package reducer;


import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class Query7aReducerFactory implements ReducerFactory<String, Set<String>, Set<String>> {

    @Override
    public Reducer<Set<String>, Set<String>> newReducer(String department) {
        return new Query7aReducer();
    }

    private class Query7aReducer extends Reducer<Set<String>, Set<String>> {

        private Set<String> provinces;

        public Query7aReducer() {
            provinces = new HashSet<>();
        }

        @Override
        public void reduce(Set<String> provinces) {
            this.provinces.addAll(provinces);
        }

        @Override
        public Set<String> finalizeReduce() {
            return provinces;
        }
    }
}
