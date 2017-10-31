package collator;

import com.hazelcast.mapreduce.Collator;
import model.PoblatedDepartment;

import java.util.SortedSet;
import java.util.TreeSet;

public class Query2Collator implements Collator<PoblatedDepartment, SortedSet<PoblatedDepartment>> {

    private int topN;

    public Query2Collator(int topN) {
        this.topN = topN;
    }

    @Override
    public SortedSet<PoblatedDepartment> collate(Iterable<PoblatedDepartment> iterable) {
        SortedSet<PoblatedDepartment> set = new TreeSet<PoblatedDepartment>();
        for (PoblatedDepartment a : iterable) {
            set.add(a);
        }

        if (set.size() <= topN) {
            return set;
        }

        PoblatedDepartment first = set.first();
        PoblatedDepartment last = null;
        int i = 0;
        for (PoblatedDepartment a : set) {
            if (i > topN) {
                break;
            }
            last = a;
            i++;
        }
        return set.subSet(first, last);
    }

}
