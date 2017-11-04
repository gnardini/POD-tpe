package mapper;

import java.util.Map;

import model.CensoInfo;
import utils.Utils;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query51Mapper implements Mapper<String, CensoInfo, Integer, String> {

    @Override
    public void map(String key, CensoInfo value, Context<Integer, String> context) {
        final Map<String, String> provinceToRegion = Utils.provinceToRegion();
        context.emit(value.getHomeId(), provinceToRegion.get(value.getProvince()));
    }

}
