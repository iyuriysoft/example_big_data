package a.udf;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import a.udf.CIDRUtils;

public enum IP2GeoId {

    INSTANCE;

    private NavigableMap<Long, Pair<Long, Long>> map = new TreeMap<Long, Pair<Long, Long>>();

    public void init(String name) throws FileNotFoundException, IOException {

        if (!map.isEmpty())
            return;

        // try (BufferedReader br = new BufferedReader(new FileReader(name));) {
        Configuration conf = new Configuration();
        Path pt = new Path(name);
        FileSystem fs = FileSystem.get(conf);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));) {
            String line = br.readLine();
            while (line != null) {
                String[] ar = line.split(",");
                try {
                    Long ipstart = new CIDRUtils(ar[0]).getNetworkAddress2();
                    Long ipend = new CIDRUtils(ar[0]).getBroadcastAddress2();
                    Long geoid = Long.valueOf(ar[1]);
                    map.put(ipstart, Pair.of(ipend, geoid));

                } catch (Exception e) {
                    // TODO: handle exception
                    // skip problem values
                }
                line = br.readLine();
            }
        }
    }

    public Long ip2geo(Long v) {
        if (map.isEmpty()) {
            throw new RuntimeException("TreeMap is empty! Before using it function init(...) should be called");
        }
        if (v == null) {
            return 0L;
        }
        Entry<Long, Pair<Long, Long>> ee = map.floorEntry(v);
        if (ee == null || v > ee.getValue().getKey()) {
            return 0L;
        }
        return ee.getValue().getValue();
    }

}
