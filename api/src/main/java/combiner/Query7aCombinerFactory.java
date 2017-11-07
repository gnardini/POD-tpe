package combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.HashSet;
import java.util.Set;

public class Query7aCombinerFactory implements CombinerFactory<String, String, Set<String>> {

    @Override
    public Combiner<String, Set<String>> newCombiner(String key) {
        return new Query7aCombinerFactory.Query7aCombiner();
    }

    private class Query7aCombiner extends Combiner<String, Set<String>> {

        private Set<String> provinces;

        public Query7aCombiner() {
            provinces = new HashSet<>();
        }

        @Override
        public void combine(String province) {
            provinces.add(province);
        }

        @Override
        public Set<String> finalizeChunk() {
            return provinces;
        }

        @Override
        public void reset() {
            provinces = new HashSet<>();
        }
    }
}

