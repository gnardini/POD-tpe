import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;

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

import combiner.Query1CombinerFactory;
import combiner.Query2CombinerFactory;
import combiner.Query3CombinerFactory;
import combiner.Query51CombinerFactory;
import combiner.Query52CombinerFactory;
import combiner.Query6CombinerFactory;
import combiner.Query7aCombinerFactory;
import combiner.Query7bCombinerFactory;
import mapper.Query1Mapper;
import mapper.Query2Mapper;
import mapper.Query3Mapper;
import mapper.Query4aMapper;
import mapper.Query4bMapper;
import mapper.Query51Mapper;
import mapper.Query52Mapper;
import mapper.Query6Mapper;
import mapper.Query7aMapper;
import mapper.Query7bMapper;
import model.CensoInfo;
import model.PoblatedDepartment;
import model.PopulationPerRegion;
import model.RegionCount;
import reducer.Query1ReducerFactory;
import reducer.Query2ReducerFactory;
import reducer.Query3ReducerFactory;
import reducer.Query4aReducerFactory;
import reducer.Query4bReducerFactory;
import reducer.Query51ReducerFactory;
import reducer.Query52ReducerFactory;
import reducer.Query6ReducerFactory;
import reducer.Query7aReducerFactory;
import reducer.Query7bReducerFactory;

public class Client {

    private static final Logger logger = Logger.getRootLogger();
    private static final String GROUP_NAME = "53191-53202-54387-54377";
    private static PrintWriter outputWriter;

    public static void main(String[] args) throws ExecutionException, InterruptedException {

    	String addresses = System.getProperty("addresses");
    	String queryString = System.getProperty("query");
    	String inPathString = System.getProperty("inPath");
    	String outPathString = System.getProperty("outPath");
    	String timeOutPathString = System.getProperty("timeOutPath");
    	String paramN = System.getProperty("n");
    	String paramProv = System.getProperty("prov");

    	if( addresses == null || queryString == null || inPathString == null ||
    			outPathString == null || timeOutPathString == null){
    		throw new IllegalArgumentException(" addresses, query, inPath, outPath and"
    				+ " timeOutPath arguments must be present.");
    	}

        FileAppender appender = (FileAppender) logger.getAppender("Appender1");
        appender.setFile(timeOutPathString);
        appender.activateOptions();
        logger.addAppender(appender);

        try {
            outputWriter = new PrintWriter(outPathString);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        CsvReader csvReader = new CsvReader();
        logger.info("Empezando lectura de CSV de entrada");
        List<CensoInfo> censoInfos = csvReader.readCensoFromCsv(inPathString);
        logger.info("CSV de entrada leido");

        final ClientConfig ccfg = new ClientConfig();
        ccfg.getGroupConfig().setName(GROUP_NAME);
        ClientNetworkConfig netConfig = new ClientNetworkConfig();
        for(String ip : addresses.split(";")){
        	netConfig.addAddress(ip);
        }
        ccfg.setNetworkConfig(netConfig);

        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(ccfg);


        final IList<CensoInfo> list = hz.getList( GROUP_NAME + "-default" );
        if (list.isEmpty()) {
            logger.info("Subiendo informacion a Hazelcast");
            censoInfos.forEach(list::add);
            logger.info("Informacion subida a Hazelcast");
        }

        int query;
        try{
        	query = Integer.parseInt(queryString);
        	if(query<1 || query>7){
        		throw new Exception();
        	}
        }catch(Exception e){
        	throw new IllegalArgumentException("query argument must be a integer from 1 to 7");
        }

        switch(query){
        	case 1:
        		query1(hz, list);
        		break;
        	case 2:
        		if(paramProv == null || paramN == null){
        			throw new IllegalArgumentException("prov and n arguments must be present for"
        					+ "query 2");
        		}
        		query2(hz, list, paramProv, Integer.parseInt(paramN));
        		break;
        	case 3:
        		query3(hz, list);
        		break;
        	case 4:
        		query4(hz, list);
        		break;
        	case 5:
        		query5(hz, list);
        		break;
        	case 6:
        		if(paramN == null){
        			throw new IllegalArgumentException("n argument must be present for"
        					+ "query 6");
        		}
        		query6(hz, list, Integer.parseInt(paramN));
        	case 7:
        		if(paramN == null){
        			throw new IllegalArgumentException("n argument must be present for"
        					+ "query 7");
        		}
        		query7(hz, list, Integer.parseInt(paramN));
        }

        outputWriter.close();
        System.exit(0);
    }

    private static void query1(
            final HazelcastInstance hz,
            final IList<CensoInfo> censoInfos) throws ExecutionException, InterruptedException {
        logger.info("Empezando map/reduce para la query 1");

        JobTracker jobTracker = hz.getJobTracker(GROUP_NAME + "-query1");
        final KeyValueSource<String, CensoInfo> source = KeyValueSource.fromList(censoInfos);
        Job<String, CensoInfo> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, RegionCount>> future = job
                .mapper(new Query1Mapper())
                .combiner(new Query1CombinerFactory())
                .reducer(new Query1ReducerFactory())
                .submit();
        Map<String, RegionCount> result = future.get();

        List<Map.Entry<String, RegionCount>> entrySet = new ArrayList<>(result.entrySet());
        Collections.sort(entrySet, Comparator.comparingInt(e -> -e.getValue().getCount()));
        entrySet.forEach(r -> outputWriter.println(r.getKey() + "," + r.getValue().getCount()));
        logger.info("Termino el map/reduce para la query 1");
    }

    private static void query2(
            final HazelcastInstance hz,
            final IList<CensoInfo> censoInfos,
            final String province,
            final int top) throws ExecutionException, InterruptedException {
        logger.info("Empezando map/reduce para la query 2");

        JobTracker jobTracker = hz.getJobTracker(GROUP_NAME + "-query2");
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
        departments.forEach(outputWriter::println);
        logger.info("Termino el map/reduce para la query 2");
    }

    private static void query3(
            final HazelcastInstance hz,
            final IList<CensoInfo> censoInfos) throws ExecutionException, InterruptedException {
        logger.info("Empezando map/reduce para la query 3");

        JobTracker jobTracker = hz.getJobTracker(GROUP_NAME + "-query3");
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
        sortedResult.forEach(r -> outputWriter.println(r.getKey() + "," + String.format("%.2f", r.getValue())));
        logger.info("Termino el map/reduce para la query 3");
    }

    private static void query4(
            final HazelcastInstance hz,
            final IList<CensoInfo> censoInfos) throws ExecutionException, InterruptedException {
        logger.info("Empezando map/reduce para la query 4");
        JobTracker jobTracker = hz.getJobTracker(GROUP_NAME + "-query4");

        final KeyValueSource<String, CensoInfo> source = KeyValueSource.fromList(censoInfos);
        Job<String, CensoInfo> jobA = jobTracker.newJob(source);
        Map<Integer, String> midResult = jobA
                .mapper(new Query4aMapper())
                //.combiner(new Query4aMapper())
                .reducer(new Query4aReducerFactory())
                .submit()
                .get();

        IMap<Integer, String> bData = hz.getMap(GROUP_NAME + "-query4map");
        bData.putAll(midResult);

        final KeyValueSource<Integer, String> sourceB = KeyValueSource.fromMap(bData);
        Job<Integer, String> jobB = jobTracker.newJob(sourceB);
        Map<String, Integer> result = jobB
                .mapper(new Query4bMapper())
                //.combiner(new Query4aMapper())
                .reducer(new Query4bReducerFactory())
                .submit()
                .get();

        List<Map.Entry<String, Integer>> sortedResult = new ArrayList<>(result.entrySet());
        Collections.sort(sortedResult, Comparator.comparingInt(x -> -x.getValue()));
        sortedResult.forEach(r -> outputWriter.println(r.getKey() + " " + r.getValue()));
        logger.info("Termino el map/reduce para la query 4");
    }

    private static void query5(
            final HazelcastInstance hz,
            final IList<CensoInfo> censoInfos) throws ExecutionException, InterruptedException {
        logger.info("Empezando map/reduce para la query 5");

        JobTracker jobTracker = hz.getJobTracker(GROUP_NAME + "-query5");
        final KeyValueSource<String, CensoInfo> source1 = KeyValueSource.fromList(censoInfos);
        Job<String, CensoInfo> job1 = jobTracker.newJob(source1);
        ICompletableFuture<Map<Integer, PopulationPerRegion>> future1 = job1
                .mapper(new Query51Mapper())
                .combiner(new Query51CombinerFactory())
                .reducer(new Query51ReducerFactory())
                .submit();

        final IMap<Integer, PopulationPerRegion> map = hz.getMap(GROUP_NAME + "-query51map");
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
        sortedResult.forEach(r -> outputWriter.println(r.getKey() + "," + String.format("%.2f", r.getValue())));
        logger.info("Termino el map/reduce para la query 5");
    }

    private static void query6(
            final HazelcastInstance hz,
            final IList<CensoInfo> censoInfos,
            final int n) throws ExecutionException, InterruptedException {
        logger.info("Empezando map/reduce para la query 6");

        JobTracker jobTracker = hz.getJobTracker(GROUP_NAME + "-query6");
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
        sortedResult.forEach(r -> outputWriter.println(r.getKey() + "," + r.getValue()));
        logger.info("Termino el map/reduce para la query 6");
    }

    private static void query7(
            final HazelcastInstance hz,
            final IList<CensoInfo> censoInfos,
            final int n) throws ExecutionException, InterruptedException {
        logger.info("Empezando map/reduce para la query 7");

        JobTracker jobTracker = hz.getJobTracker(GROUP_NAME + "-query7");
        final KeyValueSource<String, CensoInfo> source = KeyValueSource.fromList(censoInfos);
        Job<String, CensoInfo> jobA = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Set<String>>> future = jobA
                .mapper(new Query7aMapper())
                .combiner(new Query7aCombinerFactory())
                .reducer(new Query7aReducerFactory())
                .submit();



        Map<String, Set<String>> result = future.get();
        final IMap<String, Set<String>> partbMap = hz.getMap( GROUP_NAME + "-query7b" );
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
        sortedResult.forEach(r -> outputWriter.println(r.getKey() + "," + r.getValue()));
        logger.info("Termino el map/reduce para la query 7");
    }

}
