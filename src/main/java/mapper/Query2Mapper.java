package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.CensoInfo;

public class Query2Mapper implements Mapper<String, CensoInfo, String, Long> {

    private final String province;

    public Query2Mapper(String province) {
        this.province = province;
    }

    @Override
    public void map(String department, CensoInfo info, Context<String, Long> context) {
        if (province.equals(info.getProvince())) {
            context.emit(department, 1L);
        }
    }

}
