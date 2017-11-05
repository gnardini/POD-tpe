package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.CensoInfo;
import utils.Utils;

public class Query4aMapper implements Mapper<String, CensoInfo, Integer, String> {

    @Override
    public void map(String s, CensoInfo censoInfo, Context<Integer, String> context) {
        String value = Utils.regionMap.get(censoInfo.getProvince());
        context.emit(censoInfo.getHomeId(), value);
    }
}
