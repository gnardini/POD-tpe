import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import combiner.*;
import mapper.*;
import model.CensoInfo;
import model.PoblatedDepartment;
import model.PopulationPerRegion;
import model.RegionCount;
import reducer.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class DistributedMap {

    private static final String CENSO_INFO_PATH = "census1000000.csv";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        CsvReader csvReader = new CsvReader();
        List<CensoInfo> censoInfos = csvReader.readCensoFromCsv(CENSO_INFO_PATH);

        final ClientConfig ccfg = new ClientConfig();
        ccfg.getGroupConfig().setName("grupo3").setPassword("12345");
        ClientNetworkConfig cnc = new ClientNetworkConfig();
        cnc.addAddress("192.168.0.13");
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
        query3(hz, list);
        query5(hz, list);
        query6(hz, list, 5);
        query7(hz, list, 4);
        System.out.println((System.nanoTime() - time) / 1E9);

        System.exit(0);
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

        Map<String, Double> result = future.get();
        List<Map.Entry<String, Double>> sortedResult = new ArrayList<>(result.entrySet());
        Collections.sort(sortedResult, Comparator.comparingDouble(x -> -x.getValue()));
        sortedResult.forEach(r -> System.out.println(r.getKey() + " " + String.format("%.2f", r.getValue())));
    }


    private static void query5(
            final HazelcastInstance hz,
            final IList<CensoInfo> censoInfos) throws ExecutionException, InterruptedException {
        System.out.println("Query 5");
        JobTracker jobTracker = hz.getJobTracker("query5");

        final KeyValueSource<String, CensoInfo> source1 = KeyValueSource.fromList(censoInfos);
        Job<String, CensoInfo> job1 = jobTracker.newJob(source1);
        ICompletableFuture<Map<Integer, PopulationPerRegion>> future1 = job1
                .mapper(new Query51Mapper())
                .combiner(new Query51CombinerFactory())
                .reducer(new Query51ReducerFactory())
                .submit();
//        future.andThen( buildCallback() );

        final IMap<Integer, PopulationPerRegion> map = hz.getMap("query51map");
        future1.get().forEach(map::put);
        final KeyValueSource<Integer, PopulationPerRegion> source2 = KeyValueSource.fromMap(map);
        Job<Integer, PopulationPerRegion> job2 = jobTracker.newJob(source2);
        ICompletableFuture<Map<String, Double>> future2 = job2
                .mapper(new Query52Mapper())
                .combiner(new Query52CombinerFactory())
                .reducer(new Query52ReducerFactory())
                .submit();
        
        Map<String, Double> result = future2.get();
        List<Map.Entry<String, Double>> sortedResult = new ArrayList<>(result.entrySet());
        Collections.sort(sortedResult, Comparator.comparingDouble(x -> -x.getValue()));
        sortedResult.forEach(r -> System.out.println(r.getKey() + " " + String.format("%.2f", r.getValue())));   
    }
    
    private static void query6(
            final HazelcastInstance hz,
            final IList<CensoInfo> censoInfos,
            final int n) throws ExecutionException, InterruptedException {
        System.out.println("Query 6");
        JobTracker jobTracker = hz.getJobTracker("query6");

        final KeyValueSource<String, CensoInfo> source = KeyValueSource.fromList(censoInfos);
        Job<String, CensoInfo> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new Query6Mapper())
                .combiner(new Query6CombinerFactory())
                .reducer(new Query6ReducerFactory())
                .submit();

        Map<String, Integer> result = future.get();
        List<Map.Entry<String, Integer>> sortedResult = new ArrayList<>(result.entrySet());
        sortedResult = sortedResult.stream().filter(entry -> entry.getValue() >= n).collect(Collectors.toList());
        Collections.sort(sortedResult, Comparator.comparingInt(x -> -x.getValue()));
        sortedResult.forEach(r -> System.out.println(r.getKey() + " " + r.getValue()));
    }

    private static void query7(
            final HazelcastInstance hz,
            final IList<CensoInfo> censoInfos,
            final int n) throws ExecutionException, InterruptedException {
        System.out.println("Query 7");
        JobTracker jobTracker = hz.getJobTracker("query7");

        final KeyValueSource<String, CensoInfo> source = KeyValueSource.fromList(censoInfos);
        Job<String, CensoInfo> jobA = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Set<String>>> future = jobA
                .mapper(new Query7aMapper())
                .combiner(new Query7aCombinerFactory())
                .reducer(new Query7aReducerFactory())
                .submit();



        Map<String, Set<String>> result = future.get();
        final IMap<String, Set<String>> partbMap = hz.getMap( "query7b" );
        result.entrySet().forEach(entry -> partbMap.put(entry.getKey(), entry.getValue()));

        final KeyValueSource<String, Set<String>> partbSource = KeyValueSource.fromMap(partbMap);
        Job<String, Set<String>> jobB = jobTracker.newJob(partbSource);
        ICompletableFuture<Map<String, Integer>> partBFuture = jobB
                .mapper(new Query7bMapper())
                .combiner(new Query7bCombinerFactory())
                .reducer(new Query7bReducerFactory())
                .submit();

        Map<String, Integer> finalResult = partBFuture.get();
        List<Map.Entry<String, Integer>> sortedResult = new ArrayList<>(finalResult.entrySet());
        sortedResult = sortedResult.stream().filter(entry -> entry.getValue() >= n).collect(Collectors.toList());
        Collections.sort(sortedResult, Comparator.comparingInt(x -> -x.getValue()));
        sortedResult.forEach(r -> System.out.println(r.getKey() + " " + r.getValue()));
    }

}
