package a.udf;

import java.net.UnknownHostException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@Description(name = "getIP", value = "_FUNC_(str) - get IP as long", extended = "Example:\n"
    + "  > SELECT getIP(ip) FROM a;\n" + "  16843263")
public class GetIP extends UDF {

  public LongWritable evaluate(Text ip) throws UnknownHostException {
    LongWritable to_value = new LongWritable(0);
    if (ip != null) {
       to_value.set(CIDRUtils.dot2LongIP(ip.toString()));
    }
    return to_value;
  }

}
