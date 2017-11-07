package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query4Mapper implements Mapper<Integer, String, String, Integer> {

    @Override
    public void map(Integer i, String s, Context<String, Integer> context) {
        context.emit(s, 1);
    }
}
