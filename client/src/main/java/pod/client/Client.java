package pod.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import combiner.*;
import mapper.*;
import model.CensoInfo;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import reducer.*;
import utils.Timer;
import utils.Utils;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Client {

    private static final Logger logger = Logger.getRootLogger();
    private static final String GROUP_NAME = "53191-53202-54387-54377";
    private static PrintWriter outputWriter;
    private static CsvReader csvReader;

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

        csvReader = new CsvReader(inPathString);

        FileAppender appender = (FileAppender) logger.getAppender("Appender1");
        appender.setFile(timeOutPathString);
        appender.activateOptions();
        logger.addAppender(appender);

        try {
            outputWriter = new PrintWriter(outPathString);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


        final ClientConfig ccfg = new ClientConfig();
        ccfg.getGroupConfig().setName(GROUP_NAME);
        ClientNetworkConfig netConfig = new ClientNetworkConfig();
        for(String ip : addresses.split(";")){
            netConfig.addAddress(ip);
        }
        ccfg.setNetworkConfig(netConfig);

        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(ccfg);

        int query;
        try{
            query = Integer.parseInt(queryString);
            if(query<1 || query>7){
                throw new Exception();
            }
        }catch(Exception e){
            throw new IllegalArgumentException("query argument must be a integer from 1 to 7");
        }

        long start = System.nanoTime();
        switch(query){
            case 1:
                query1(hz);
                break;
            case 2:
                if(paramProv == null || paramN == null){
                    throw new IllegalArgumentException("prov and n arguments must be present for"
                            + "query 2");
                }
                query2(hz, paramProv, Integer.parseInt(paramN));
                break;
            case 3:
                query3(hz);
                break;
            case 4:
                query4(hz);
                break;
            case 5:
                query5(hz);
                break;
            case 6:
                if(paramN == null){
                    throw new IllegalArgumentException("n argument must be present for"
                            + "query 6");
                }
                query6(hz, Integer.parseInt(paramN));
            case 7:
                if(paramN == null){
                    throw new IllegalArgumentException("n argument must be present for"
                            + "query 7");
                }
                query7(hz, Integer.parseInt(paramN));
        }
        double diffSecs = (System.nanoTime() - start) / 1E9;
        logger.info("Query " + query + " tardo: " + diffSecs + " segundos.");

        outputWriter.close();
        System.exit(0);
    }

    private static void query1(final HazelcastInstance hz) throws ExecutionException, InterruptedException {
        Timer timer = new Timer(logger);
        final MultiMap<String, CensoInfo> map = hz.getMultiMap( GROUP_NAME + "-query1map" );
        map.clear();
        logger.info("Empezando lectura de CSV de entrada");
        Map<String, String> regions = Utils.provinceToRegion();
        csvReader.populateMultiMap(map, c -> regions.get(c.getProvince()), c -> c);
        logger.info("CSV de entrada subido a Hazelcast");
        timer.end("Tiempo de carga de query 1:");

        logger.info("Empezando map/reduce para la query 1");
        JobTracker jobTracker = hz.getJobTracker(GROUP_NAME + "-query1");
        final KeyValueSource<String, CensoInfo> source = KeyValueSource.fromMultiMap(map);
        Job<String, CensoInfo> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new Query1Mapper())
                .combiner(new Query1CombinerFactory())
                .reducer(new Query1ReducerFactory())
                .submit();
        Map<String, Integer> result = future.get();

        List<Map.Entry<String, Integer>> entrySet = new ArrayList<>(result.entrySet());
        Collections.sort(entrySet, Comparator.comparingLong(e -> -e.getValue()));
        entrySet.forEach(r -> outputWriter.println(r.getKey() + "," + r.getValue()));
        logger.info("Termino el map/reduce para la query 1");
        timer.end("Map/reduce query 1:");
    }

    private static void query2(
            final HazelcastInstance hz,
            final String province,
            final int top) throws ExecutionException, InterruptedException {
        Timer timer = new Timer(logger);
        final MultiMap<String, CensoInfo> map = hz.getMultiMap( GROUP_NAME + "-query2map" );
        map.clear();
        logger.info("Empezando lectura de CSV de entrada");
        csvReader.populateMultiMap(map, CensoInfo::getDepartment, c -> c);
        logger.info("CSV de entrada subido a Hazelcast");
        timer.end("Tiempo de carga de query 2:");

        logger.info("Empezando map/reduce para la query 2");
        JobTracker jobTracker = hz.getJobTracker(GROUP_NAME + "-query2");
        final KeyValueSource<String, CensoInfo> source = KeyValueSource.fromMultiMap(map);
        Job<String, CensoInfo> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query2Mapper(province))
                .combiner(new Query2CombinerFactory())
                .reducer(new Query2ReducerFactory())
                .submit();

        Map<String, Long> result = future.get();

        List<Map.Entry<String, Long>> entrySet = new ArrayList<>(result.entrySet());
        Collections.sort(entrySet, Comparator.comparingLong(e -> -e.getValue()));
        if (entrySet.size() > top) {
            entrySet = entrySet.subList(0, top);
        }
        entrySet.forEach(r -> outputWriter.println(r.getKey() + "," + r.getValue()));
        logger.info("Termino el map/reduce para la query 2");
        timer.end("Map/reduce query 2:");
    }

    private static void query3(
            final HazelcastInstance hz) throws ExecutionException, InterruptedException {
        Timer timer = new Timer(logger);
        final MultiMap<String, CensoInfo> map = hz.getMultiMap( GROUP_NAME + "-query3map" );
        map.clear();
        logger.info("Empezando lectura de CSV de entrada");
        Map<String, String> regions = Utils.provinceToRegion();
        csvReader.populateMultiMap(map, c -> regions.get(c.getProvince()), c -> c);
        logger.info("CSV de entrada subido a Hazelcast");
        timer.end("Tiempo de carga de query 3:");

        logger.info("Empezando map/reduce para la query 3");
        JobTracker jobTracker = hz.getJobTracker(GROUP_NAME + "-query3");
        final KeyValueSource<String, CensoInfo> source = KeyValueSource.fromMultiMap(map);
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
        timer.end("Map/reduce query 3:");
    }

    private static void query4(
            final HazelcastInstance hz) throws ExecutionException, InterruptedException {
        Timer timer = new Timer(logger);
        final IMap<Integer, String> map = hz.getMap( GROUP_NAME + "-query4map" );
        map.clear();
        logger.info("Empezando lectura de CSV de entrada");
        Map<String, String> regions = Utils.provinceToRegion();
        csvReader.populateMap(map, CensoInfo::getHomeId, c -> regions.get(c.getProvince()));
        logger.info("CSV de entrada subido a Hazelcast");
        timer.end("Tiempo de carga de query 4:");

        logger.info("Empezando map/reduce para la query 4");
        JobTracker jobTracker = hz.getJobTracker(GROUP_NAME + "-query4");

        final KeyValueSource<Integer, String> source = KeyValueSource.fromMap(map);
        Job<Integer, String> job = jobTracker.newJob(source);
        Map<String, Integer> result = job
                .mapper(new Query4Mapper())
                .combiner(new Query4CombinerFactory())
                .reducer(new Query4ReducerFactory())
                .submit()
                .get();

        List<Map.Entry<String, Integer>> sortedResult = new ArrayList<>(result.entrySet());
        Collections.sort(sortedResult, Comparator.comparingInt(x -> -x.getValue()));
        sortedResult.forEach(r -> outputWriter.println(r.getKey() + " " + r.getValue()));
        logger.info("Termino el map/reduce para la query 4");
        timer.end("Map/reduce query 4:");
    }

    private static void query5(
            final HazelcastInstance hz) throws ExecutionException, InterruptedException {
        Timer timer = new Timer(logger);
        final MultiMap<Integer, CensoInfo> map = hz.getMultiMap( GROUP_NAME + "-query5map" );
        map.clear();
        logger.info("Empezando lectura de CSV de entrada");
        csvReader.populateMultiMap(map, CensoInfo::getHomeId, c -> c);
        logger.info("CSV de entrada subido a Hazelcast");
        timer.end("Tiempo de carga de query 5:");

        logger.info("Empezando map/reduce para la query 5");
        JobTracker jobTracker = hz.getJobTracker(GROUP_NAME + "-query5");
        final KeyValueSource<Integer, CensoInfo> source = KeyValueSource.fromMultiMap(map);
        Job<Integer, CensoInfo> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Double>> future = job
                .mapper(new Query5Mapper())
                .combiner(new Query5CombinerFactory())
                .reducer(new Query5ReducerFactory())
                .submit();

        Map<String, Double> result = future.get();
        List<Map.Entry<String, Double>> sortedResult = new ArrayList<>(result.entrySet());
        Collections.sort(sortedResult, Comparator.comparingDouble(x -> -x.getValue()));
        sortedResult.forEach(r -> outputWriter.println(r.getKey() + "," + String.format("%.2f", r.getValue())));
        logger.info("Termino el map/reduce para la query 5");
        timer.end("Map/reduce query 5:");
    }

    private static void query6(
            final HazelcastInstance hz,
            final int n) throws ExecutionException, InterruptedException {
        Timer timer = new Timer(logger);
        final MultiMap<String, String> map = hz.getMultiMap( GROUP_NAME + "-query6map" );
        map.clear();
        logger.info("Empezando lectura de CSV de entrada");
        csvReader.populateMultiMap(map, CensoInfo::getDepartment, CensoInfo::getProvince);
        logger.info("CSV de entrada subido a Hazelcast");
        timer.end("Tiempo de carga de query 6:");

        logger.info("Empezando map/reduce para la query 6");
        JobTracker jobTracker = hz.getJobTracker(GROUP_NAME + "-query6");
        final KeyValueSource<String, String> source = KeyValueSource.fromMultiMap(map);
        Job<String, String> job = jobTracker.newJob(source);
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
        timer.end("Map/reduce query 6:");
    }

    private static void query7(
            final HazelcastInstance hz,
            final int n) throws ExecutionException, InterruptedException {
        Timer timer = new Timer(logger);
        final MultiMap<String, String> map = hz.getMultiMap( GROUP_NAME + "-query7map" );
        map.clear();
        logger.info("Empezando lectura de CSV de entrada");
        csvReader.populateMultiMap(map, CensoInfo::getDepartment, CensoInfo::getProvince);
        logger.info("CSV de entrada subido a Hazelcast");
        timer.end("Tiempo de carga de query 7: ");

        logger.info("Empezando map/reduce para la query 7");
        JobTracker jobTracker = hz.getJobTracker(GROUP_NAME + "-query7");
        final KeyValueSource<String, String> source = KeyValueSource.fromMultiMap(map);
        Job<String, String> jobA = jobTracker.newJob(source);
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
        timer.end("Map/reduce query 7:");
    }

}
