import collator.Query2Collator;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import combiner.Query1CombinerFactory;
import combiner.Query2CombinerFactory;
import mapper.Query1Mapper;
import mapper.Query2Mapper;
import model.CensoInfo;
import model.PoblatedDepartment;
import model.RegionCount;
import reducer.Query1ReducerFactory;
import reducer.Query2ReducerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class DistributedMap {

    private static final String CENSO_INFO_PATH = "/Users/Viki/Documents/workspace/itba/pod/POD-tpe/census100.csv";

    private static final Comparator<Map.Entry<String, RegionCount>> ENTRYSET_COMPARATOR = new Comparator<Map.Entry<String, RegionCount>>() {
        @Override
        public int compare(Map.Entry<String, RegionCount> o1, Map.Entry<String, RegionCount> o2) {
            Integer i1 = o1.getValue().getCount();
            Integer i2 = o2.getValue().getCount();
            return i1.compareTo(i2);
        }
    };

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        CsvReader csvReader = new CsvReader();
        List<CensoInfo> censoInfos = csvReader.readCensoFromCsv(CENSO_INFO_PATH);

        /*final ClientConfig ccfg = new ClientConfig();
        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(ccfg);

        JobTracker jobTracker = hz.getJobTracker("query1");

        final IList<CensoInfo> list = hz.getList( "censo-infos" );
        censoInfos.forEach(list::add);

        final KeyValueSource<String, CensoInfo> source = KeyValueSource.fromList(list);
        Job<String, CensoInfo> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, RegionCount>> future = job
                .mapper(new Query1Mapper())
                .combiner(new Query1CombinerFactory())
                .reducer(new Query1ReducerFactory())
                .submit();
//        future.andThen( buildCallback() );
        Map<String, RegionCount> result = future.get();

        List<Map.Entry<String, RegionCount>> entrySet = new ArrayList<>(result.entrySet());
        Collections.sort(entrySet, ENTRYSET_COMPARATOR);
        entrySet.forEach(r -> System.out.println(r.getKey() + "," + r.getValue().getCount()));*/

        final ClientConfig ccfg = new ClientConfig();
        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(ccfg);

        JobTracker jobTracker = hz.getJobTracker("query2");

        final IList<CensoInfo> list = hz.getList( "censo-infos" );
        censoInfos.forEach(list::add);
        final KeyValueSource<String, CensoInfo> source = KeyValueSource.fromList(list);
        Job<String, CensoInfo> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, PoblatedDepartment>> future = job
                .mapper(new Query2Mapper())
                .reducer(new Query2ReducerFactory())
                .submit();
//        future.andThen( buildCallback() );

        Map<String, PoblatedDepartment> result = future.get();
        Query2Collator collator = new Query2Collator(10);
        SortedSet<PoblatedDepartment> departments = collator.collate(result.values());
        departments.forEach(r -> System.out.println(r));

        list.clear();

    }

}
