import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
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

import static java.lang.System.exit;

public class DistributedMap {

    private static final String CENSO_INFO_PATH = "census1000000.csv";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        CsvReader csvReader = new CsvReader();
        List<CensoInfo> censoInfos = csvReader.readCensoFromCsv(CENSO_INFO_PATH);

        final ClientConfig ccfg = new ClientConfig();
        ccfg.getGroupConfig().setName("grupo3").setPassword("12345");
        ClientNetworkConfig cnc = new ClientNetworkConfig();
        cnc.addAddress("192.168.0.13");
        cnc.addAddress("192.168.0.23");
        ccfg.setNetworkConfig(cnc);

        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(ccfg);

        final IList<CensoInfo> list = hz.getList( "default" );
        if (list.isEmpty()) {
            System.out.println("Adding censo info");
            censoInfos.forEach(list::add);
            System.out.println("Censo info added");
        }

        long time = System.nanoTime();
        query1(hz, list);
        query2(hz, list, "Santa Fe", 10);
        System.out.println((System.nanoTime() - time) / 1E9);

        exit(0);
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
        Map<String, RegionCount> result = future.get();

        List<Map.Entry<String, RegionCount>> entrySet = new ArrayList<>(result.entrySet());
        Collections.sort(entrySet, Comparator.comparingInt(e -> e.getValue().getCount()));
        entrySet.forEach(r -> System.out.println(r.getKey() + ", " + r.getValue().getCount()));
    }

    private static void query2(
            final HazelcastInstance hz,
            final IList<CensoInfo> censoInfos,
            final String province,
            final int top) throws ExecutionException, InterruptedException {
        System.out.println("Query 2");
        JobTracker jobTracker = hz.getJobTracker("query2");

        final KeyValueSource<String, CensoInfo> source = KeyValueSource.fromList(censoInfos);
        Job<String, CensoInfo> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, PoblatedDepartment>> future = job
                .mapper(new Query2Mapper(province))
                .combiner(new Query2CombinerFactory())
                .reducer(new Query2ReducerFactory())
                .submit();

        Map<String, PoblatedDepartment> result = future.get();
        List<PoblatedDepartment> departments = new ArrayList<>(result.values());
        Collections.sort(departments);
        if (departments.size() > top) {
            departments = departments.subList(0, top);
        }
        departments.forEach(System.out::println);
    }

}
