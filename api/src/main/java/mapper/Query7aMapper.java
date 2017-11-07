package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query7aMapper implements Mapper<String, String, String, String> {

    @Override
    public void map(String department, String province, Context<String, String> context) {
        context.emit(department, province);
    }
}

