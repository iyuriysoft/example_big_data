package a.b;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.LoggerFactory;

public class FlumeRPCClient {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(FlumeRPCClient.class);
  
  public static void main(String[] args) {
    MyRpcClientFacade1 client = new MyRpcClientFacade1();
    // Initialize client with the remote Flume agent's host and port
    if (args.length < 3) {
      System.out.println("example: program.jar 127.0.0.1 4445 input.txt");
      return;
    }
    client.init(args[0], Integer.valueOf(args[1]));

    // Send 10 events to the remote Flume agent. That agent should be
    // configured to listen with an Thrift Source
    Path file = Paths.get(args[2]);
    try (InputStream in = Files.newInputStream(file);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
      String line;
      while ((line = reader.readLine()) != null) {
        logger.debug(String.format("DATA: %s", line));
        String[] ar = line.split(",");
        if (ar.length < 1)
          break;
        client.sendDataToFlume(line);
      }
    } catch (IOException e) {
      logger.error("ERROR: ", e);
    }

    client.cleanUp();
  }
}

class MyRpcClientFacade1 {
  private RpcClient client;
  private String hostname;
  private int port;

  public void init(String hostname, int port) {
    // Setup the RPC connection
    this.hostname = hostname;
    this.port = port;
    // this.client = RpcClientFactory.getDefaultInstance(hostname, port);
    // Use the following method to create a thrift client (instead of the
    // above line):
    this.client = RpcClientFactory.getThriftInstance(hostname, port);
  }

  public void sendDataToFlume(String data) {
    // Create a Flume Event object that encapsulates the sample data
    Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));

    // Send the event
    try {
      client.append(event);
    } catch (EventDeliveryException e) {
      // clean up and recreate the client
      client.close();
      client = null;
      // client = RpcClientFactory.getDefaultInstance(hostname, port);
      // Use the following method to create a thrift client (instead of
      // the above line):
      client = RpcClientFactory.getThriftInstance(hostname, port);
    }
  }

  public void cleanUp() {
    // Close the RPC connection
    client.close();
  }

  public String getRandomString() {
    return "";
  }
}
