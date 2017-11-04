package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.CensoInfo;
import model.PoblatedDepartment;

public class Query2Mapper implements Mapper<String, CensoInfo, String, PoblatedDepartment> {

    @Override
    public void map(String key, CensoInfo value, Context<String, PoblatedDepartment> context) {
        context.emit(value.getDepartment(), new PoblatedDepartment(value.getDepartment(), new Long(1)));
    }

}
