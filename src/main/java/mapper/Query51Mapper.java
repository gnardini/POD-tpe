package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.CensoInfo;
import utils.Utils;

import java.util.Map;

public class Query51Mapper implements Mapper<Integer, CensoInfo, Integer, String> {

    private static final Map<String, String> regionMap = Utils.provinceToRegion();

    @Override
    public void map(Integer homeId, CensoInfo info, Context<Integer, String> context) {
        context.emit(homeId, regionMap.get(info.getProvince()));
    }

}
