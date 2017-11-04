package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.CensoInfo;

public class Query6Mapper implements Mapper<String, CensoInfo, String, String> {

    @Override
    public void map(String s, CensoInfo censoInfo, Context<String, String> context) {
        context.emit(censoInfo.getDepartment(), censoInfo.getProvince());
    }
}
