package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.CensoInfo;
import utils.Utils;

import java.util.Map;

public class Query51Mapper implements Mapper<String, CensoInfo, Integer, String> {

    final Map<String, String> provinceToRegion = Utils.provinceToRegion();

    @Override
    public void map(String key, CensoInfo value, Context<Integer, String> context) {
        context.emit(value.getHomeId(), provinceToRegion.get(value.getProvince()));
    }

}
