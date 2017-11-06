import com.hazelcast.core.IList;
import model.CensoInfo;
import model.Condition;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CsvReader {

    public void readCensoFromCsv(String csvFile, IList<CensoInfo> iList) {
        String line;
        String cvsSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            CensoInfo censoInfo;
            while ((line = br.readLine()) != null) {
                String[] items = line.split(cvsSplitBy);
                censoInfo = new CensoInfo(Condition.fromInt(Integer.parseInt(items[0])), Integer.parseInt(items[1]), items[2], items[3]);
                iList.add(censoInfo);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
