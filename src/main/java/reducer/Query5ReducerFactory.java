package reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import model.RegionInfo;

public class Query5ReducerFactory implements ReducerFactory<String, RegionInfo, Double> {

    @Override
    public Reducer<RegionInfo, Double> newReducer(String region) {
        return new Query5Reducer();
    }

    private class Query5Reducer extends Reducer<RegionInfo, Double> {

        private RegionInfo regionInfo = new RegionInfo();

        @Override
        public void reduce(RegionInfo region) {
            regionInfo.addRegionInfo(region);
        }

        @Override
        public Double finalizeReduce() {
            return regionInfo.getPeoplePerHome();
        }
    }

}
