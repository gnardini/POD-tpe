package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.CensoInfo;
import utils.Utils;

import java.util.Map;

public class Query5Mapper implements Mapper<Integer, CensoInfo, String, Integer> {

    private static final Map<String, String> regionMap = Utils.provinceToRegion();

    @Override
    public void map(Integer homeId, CensoInfo info, Context<String, Integer> context) {
        context.emit(regionMap.get(info.getProvince()), homeId);
    }

}
