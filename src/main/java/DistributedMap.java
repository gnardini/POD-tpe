import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import combiner.Query1CombinerFactory;
import mapper.Query1Mapper;
import model.CensoInfo;
import model.RegionCount;
import reducer.Query1ReducerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class DistributedMap {

    private static final String CENSO_INFO_PATH = "/Users/gnardini/Documents/Code/pod-tpe/census100.csv";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        CsvReader csvReader = new CsvReader();
        List<CensoInfo> censoInfos = csvReader.readCensoFromCsv(CENSO_INFO_PATH);

        final ClientConfig ccfg = new ClientConfig();
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
        result.values().forEach(r -> System.out.println(r.getRegion() + "," + r.getCount()));
    }

}
