package a.udf;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import a.udf.CIDRUtils;

@Description(name = "getGeoIdFromIP", value = "long _FUNC_(str) - get geoId from ip", extended = "Example:\n  > SELECT getGeoIdFromIP(ip) FROM a;")
public class GetGeoIdFromIP extends UDF {

    public LongWritable evaluate(Text ip, Text countryip_filename) throws FileNotFoundException, IOException {
        IP2GeoId.INSTANCE.init(countryip_filename.toString());
        LongWritable to_value = new LongWritable(0);
        if (ip != null) {
            to_value.set(IP2GeoId.INSTANCE.ip2geo(CIDRUtils.dot2LongIP(ip.toString())));
        }
        return to_value;
    }

}
