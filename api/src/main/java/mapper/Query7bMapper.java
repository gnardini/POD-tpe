package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class Query7bMapper implements Mapper<String, Set<String>, String, Integer> {

    @Override
    public void map(String department, Set<String> provinces, Context<String, Integer> context) {
        List<String> sortedProvinces = new ArrayList<>(provinces);
        Collections.sort(sortedProvinces);
        for (int i = 0; i < sortedProvinces.size(); i++) {
            for (int j = i + 1; j < sortedProvinces.size(); j++) {
                String combination = sortedProvinces.get(i) + " + " + sortedProvinces.get(j);
                context.emit(combination, 1);
            }
        }
    }
}
