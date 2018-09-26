package a.b;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class CustomSource extends AbstractSource implements Configurable, PollableSource {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CustomSource.class);
  private static final String INPUT_FILE = "input.txt";
  private SimpleDateFormat dfDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private String inputFile;
  private static int first;

  public long getBackOffSleepIncrement() {
      return 0;
  }

  public long getMaxBackOffSleepInterval() {
      return 0;
  }
  /**
   *
   * @param context
   */
  @Override
  public void configure(Context context) {
    logger.warn("CONFIGURE");
    inputFile = context.getString(INPUT_FILE);
    logger.warn(String.format("FILE: %s", inputFile));
    
    ImmutableMap<String, String> parameters = context.getParameters();
    logger.info("parameters: " + parameters); 
  }

  /**
   * @return void
   */
  @Override
  public void start() {
    logger.warn("START custom flume source");
    super.start();
  }

  /**
   * @return void
   */
  @Override
  public void stop() {
    // Disconnect from external client and do any additional cleanup
    // (e.g. releasing resources or nulling-out field values) ..
    logger.warn("STOP custom flume source");
    super.stop();
  }

  /**
   * @return Status , process source configured from context
   * @throws org.apache.flume.EventDeliveryException
   */
  @Override
  public Status process() throws EventDeliveryException {
    if (first > 0)
      return Status.READY;
    Status status = Status.READY;
    logger.warn("PROCESS");

    try {
      // This try clause includes whatever Channel/Event operations you want to do
      // Receive new data
      Map<String, String> headers = new HashMap<>();
      headers.put("type", "data");
      Path file = Paths.get(inputFile);
      try (InputStream in = Files.newInputStream(file);
          BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
        String line;
        while ((line = reader.readLine()) != null) {
          logger.debug(String.format("DATA: %s", line));
          String[] ar = line.split(",");
          if (ar.length < 3)
            break;
          processLine(line.getBytes(), dfDateTime.parse(ar[2]).getTime());
        }
      } catch (IOException e) {
        logger.error("ERROR: ", e);
      }
      status = Status.READY;
    } catch (Throwable t) {
      // Log exception, handle individual exceptions as needed
      logger.error("ERROR: ", t);
      status = Status.BACKOFF;
      // re-throw all Errors
      if (t instanceof Error) {
        throw (Error)t;
      }
    }
    first = 1;
    status = Status.BACKOFF;
    return status;
  }

  /**
   * @void process file lines.
   * @param line byte[]
   */
  private void processLine(byte[] line, long timestamp) {
    byte[] message = line;
    Event event = new SimpleEvent();
    Map<String, String> headers = new HashMap<>();
    headers.put("timestamp", String.valueOf(timestamp));
    event.setBody(message);
    event.setHeaders(headers);
    try {
      getChannelProcessor().processEvent(event);
    } catch (ChannelException e) {
      logger.error("ERROR: ", e);
    }

  }
}