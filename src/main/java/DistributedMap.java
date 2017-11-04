import collator.Query2Collator;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import combiner.Query1CombinerFactory;
import combiner.Query3CombinerFactory;
import mapper.Query1Mapper;
import mapper.Query2Mapper;
import mapper.Query3Mapper;
import model.CensoInfo;
import model.EmploymentData;
import model.PoblatedDepartment;
import model.RegionCount;
import reducer.Query1ReducerFactory;
import reducer.Query2ReducerFactory;
import reducer.Query3ReducerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class DistributedMap {

    private static final String CENSO_INFO_PATH = "census1000000.csv";

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

        final ClientConfig ccfg = new ClientConfig();
        ccfg.getGroupConfig().setName("grupo3").setPassword("12345");
        ClientNetworkConfig cnc = new ClientNetworkConfig();
        //cnc.addAddress("192.168.0.13");
        cnc.addAddress("192.168.0.23");
        ccfg.setNetworkConfig(cnc);

        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(ccfg);

        System.out.println("Adding censo info");
        final IList<CensoInfo> list = hz.getList( "censo-infos" );
        censoInfos.forEach(list::add);
        System.out.println("Censo info added");

        query1(hz, list);
        query2(hz, list);
        query3(hz, list);

        list.clear();
    }

    private static void query1(
            final HazelcastInstance hz,
            final IList<CensoInfo> censoInfos) throws ExecutionException, InterruptedException {
        System.out.println("Query 1");
        JobTracker jobTracker = hz.getJobTracker("query1");

        final KeyValueSource<String, CensoInfo> source = KeyValueSource.fromList(censoInfos);
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
        entrySet.forEach(r -> System.out.println(r.getKey() + ", " + Math.log(r.getValue().getCount())));
    }

    private static void query2(
            final HazelcastInstance hz,
            final IList<CensoInfo> censoInfos) throws ExecutionException, InterruptedException {
        System.out.println("Query 2");
        JobTracker jobTracker = hz.getJobTracker("query2");

        final KeyValueSource<String, CensoInfo> source = KeyValueSource.fromList(censoInfos);
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
    }

    private static void query3(
            final HazelcastInstance hz,
            final IList<CensoInfo> censoInfos) throws ExecutionException, InterruptedException {
        System.out.println("Query 3");
        JobTracker jobTracker = hz.getJobTracker("query3");

        final KeyValueSource<String, CensoInfo> source = KeyValueSource.fromList(censoInfos);
        Job<String, CensoInfo> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Double>> future = job
                .mapper(new Query3Mapper())
                .combiner(new Query3CombinerFactory())
                .reducer(new Query3ReducerFactory())
                .submit();
//        future.andThen( buildCallback() );

        Map<String, Double> result = future.get();
        List<Map.Entry<String, Double>> sortedResult = new ArrayList<>(result.entrySet());
        Collections.sort(sortedResult, Comparator.comparingDouble(x -> -x.getValue()));
        sortedResult.forEach(r -> System.out.println(r.getKey() + " " + String.format("%.2f", r.getValue())));
    }


}
