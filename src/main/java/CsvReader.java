import model.CensoInfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class CsvReader {

    public List<CensoInfo> readCensoFromCsv(String csvFile) {
        List<CensoInfo>  censoInfoList = new LinkedList<>();

        String line;
        String cvsSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {
                String[] items = line.split(cvsSplitBy);
                CensoInfo censoInfo = new CensoInfo(Integer.parseInt(items[0]), Integer.parseInt(items[1]), items[2], items[3]);
                censoInfoList.add(censoInfo);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return censoInfoList;
    }

}
