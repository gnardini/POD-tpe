import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import model.CensoInfo;
import model.Condition;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

public class CsvReader {

    private static final String cvsSplitBy = ",";
    private BufferedReader br;
    private String fileName;

    public CsvReader(String fileName) {
        this.fileName = fileName;
    }

    public void start() {
        try {
            br = new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public synchronized CensoInfo next() {
        try {
            String line = br.readLine();
            if (line == null) {
                return null;
            }
            String[] items = line.split(cvsSplitBy);
            return new CensoInfo(Condition.fromInt(Integer.parseInt(items[0])), Integer.parseInt(items[1]), items[2], items[3]);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public <Key, Value> void populateMultiMap(
            MultiMap<Key, Value> map, Function<CensoInfo, Key> keyMapper, Function<CensoInfo, Value> valueMapper) {
        start();
        ExecutorService executor = Executors.newFixedThreadPool(100);
        try {
            List<Callable<Void>> tasks = new LinkedList<>();
            for (int i = 0; i < 100; i++) {
                tasks.add(() -> {
                    CensoInfo info;
                    while ((info = next()) != null) {
                        map.put(keyMapper.apply(info), valueMapper.apply(info));
                    }
                    return null;
                });
            }
            List<Future<Void>> futures = executor.invokeAll(tasks);
            futures.forEach(f -> {
                try {
                    f.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public <Key, Value> void populateMap(
            IMap<Key, Value> map, Function<CensoInfo, Key> keyMapper, Function<CensoInfo, Value> valueMapper) {
        start();
        ExecutorService executor = Executors.newFixedThreadPool(100);
        try {
            List<Callable<Void>> tasks = new LinkedList<>();
            for (int i = 0; i < 100; i++) {
                tasks.add(() -> {
                    CensoInfo info;
                    while ((info = next()) != null) {
                        map.putIfAbsent(keyMapper.apply(info), valueMapper.apply(info));
                    }
                    return null;
                });
            }
            List<Future<Void>> futures = executor.invokeAll(tasks);
            futures.forEach(f -> {
                try {
                    f.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
