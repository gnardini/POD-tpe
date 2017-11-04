package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.CensoInfo;
import utils.Utils;

import java.util.HashMap;
import java.util.Map;

public class Query1Mapper implements Mapper<String, CensoInfo, String, Integer> {

    @Override
    public void map(String key, CensoInfo value, Context<String, Integer> context) {
        final Map<String, String> provinceToRegion = Utils.provinceToRegion();
        String region = provinceToRegion.get(value.getProvince());
        context.emit(region, 1);
    }

}
