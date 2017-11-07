package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.CensoInfo;

public class Query1Mapper implements Mapper<String, CensoInfo, String, Integer> {

    @Override
    public void map(String region, CensoInfo info, Context<String, Integer> context) {
        context.emit(region, 1);
    }

}
