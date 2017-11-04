package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.CensoInfo;
import model.Condition;
import model.EmploymentData;
import utils.Utils;

public class Query3Mapper implements Mapper<String, CensoInfo, String, EmploymentData> {

    @Override
    public void map(String s, CensoInfo censoInfo, Context<String, EmploymentData> context) {
        String region = Utils.provinceToRegion().get(censoInfo.getProvince());
        int e = censoInfo.getCondition() == Condition.EMPLOYED ? 1 : 0;
        int u = censoInfo.getCondition() == Condition.UNEMPLOYED ? 1 : 0;
        context.emit(region, new EmploymentData(e, u));
    }
}
