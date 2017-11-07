package mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.CensoInfo;
import model.Condition;
import model.EmploymentData;

public class Query3Mapper implements Mapper<String, CensoInfo, String, EmploymentData> {

    @Override
    public void map(String region, CensoInfo info, Context<String, EmploymentData> context) {
        int e = info.getCondition() == Condition.EMPLOYED ? 1 : 0;
        int u = info.getCondition() == Condition.UNEMPLOYED ? 1 : 0;
        context.emit(region, new EmploymentData(e, u));
    }
}
