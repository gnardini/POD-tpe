package model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class EmploymentData implements DataSerializable {

    public int employedPeople;
    public int unemployedPeople;

    public EmploymentData(int employeddPeople, int unemployedPeople) {
        this.employedPeople = employeddPeople;
        this.unemployedPeople = unemployedPeople;
    }
    
    public EmploymentData() {
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(employedPeople);
        out.writeInt(unemployedPeople);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        employedPeople = in.readInt();
        unemployedPeople = in.readInt();
    }
}
