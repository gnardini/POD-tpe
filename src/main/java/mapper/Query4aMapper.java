package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.CensoInfo;
import utils.Utils;

import java.util.Map;

public class Query4aMapper implements Mapper<String, CensoInfo, Integer, String> {

    private Map<String, String> regions = Utils.provinceToRegion();

    @Override
    public void map(String s, CensoInfo censoInfo, Context<Integer, String> context) {
        String region = regions.get(censoInfo.getProvince());
        context.emit(censoInfo.getHomeId(), region);
    }
}
