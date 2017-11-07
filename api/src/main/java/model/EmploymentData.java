package model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Counter for number of people that are either employed or unemployed.
 */
public class EmploymentData implements DataSerializable {

    public int employedPeople;
    public int unemployedPeople;

    public EmploymentData(int employedPeople, int unemployedPeople) {
        this.employedPeople = employedPeople;
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
