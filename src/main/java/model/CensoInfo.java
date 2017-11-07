package model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class CensoInfo implements DataSerializable {

    private int uniqueId;
    private Condition condition;
    private int homeId;
    private String department;
    private String province;

    public CensoInfo() {
    }

    public CensoInfo(int uniqueId, Condition condition, int homeId, String department, String province) {
        this.uniqueId = uniqueId;
        this.condition = condition;
        this.homeId = homeId;
        this.department = department;
        this.province = province;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(uniqueId);
        out.writeInt(condition.ordinal());
        out.writeInt(homeId);
        out.writeUTF(department);
        out.writeUTF(province);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        uniqueId = in.readInt();
        condition = Condition.fromInt(in.readInt());
        homeId = in.readInt();
        department = in.readUTF();
        province = in.readUTF();
    }

    public Condition getCondition() {
        return condition;
    }

    public int getHomeId() {
        return homeId;
    }

    public String getDepartment() {
        return department;
    }

    public String getProvince() {
        return province;
    }

}
