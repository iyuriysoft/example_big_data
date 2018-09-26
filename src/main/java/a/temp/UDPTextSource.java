package a.temp;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.SyslogSourceConfigurationConstants;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDPTextSource extends AbstractSource implements EventDrivenSource, Configurable {

  @Override
  public void configure(Context context) {
    // TODO Auto-generated method stub
    
  }

//    private int port;
//    private int maxsize = 1 << 16; // 64k is max allowable in RFC 5426
//    private String host = null;
//    private Channel nettyChannel;
//    private SimpleDateFormat dateFormatter = new SimpleDateFormat("yyy-MM-dd HH:mm:ss");
//    
//    public static final int DEFAULT_MIN_SIZE = 4096;
//    public static final int DEFAULT_INITIAL_SIZE = DEFAULT_MIN_SIZE;
//    
//    private static final Logger logger = LoggerFactory.getLogger(UDPTextSource.class);
//    private CounterGroup counterGroup = new CounterGroup();
//
//    public class syslogHandler extends SimpleChannelHandler {
//
//        int maxsize = 1 << 16;
//        boolean doneReading = false;
//        ByteArrayOutputStream baos = new ByteArrayOutputStream(maxsize);
//        
//        private void resetReader(){
//            doneReading = false;
//            baos.reset();            
//        }
//        
//        private Event extractEvent(ChannelBuffer in){
//            byte b = 0;            
//            Event e = null;
//                       
//            try {
//                while (!doneReading && in.readable()) {
//                  b = in.readByte();                                   
//                  if(b == '\n'){                      
//                      doneReading = true;
//                      continue;
//                  }
//                  baos.write(b);
//                }
//            }catch(Exception ex){
//                resetReader();
//            }            
//            
//            if(doneReading){                
//                String str = String.join(",", baos.toString(), dateFormatter.format(new Date()));
//                e = EventBuilder.withBody(str, Charset.defaultCharset());                
//                resetReader();
//            }
//            
//            return e;    
//        }
//        
//        @Override
//        public void messageReceived(ChannelHandlerContext ctx, MessageEvent mEvent) {
//
//            try{
//                ChannelBuffer cb = (ChannelBuffer) mEvent.getMessage();
//                Event e = extractEvent(cb);
//                
//                if(e == null){
//                    return;
//                }
//                
//                getChannelProcessor().processEvent(e);
//            }catch(ChannelException ex) {
//                counterGroup.incrementAndGet("events.dropped");
//                logger.error("Error writting to channel", ex);
//                return;
//            }
//            
//        }
//
//    }
//
//    @Override
//    public void start() {
//        // setup Netty server
//        ConnectionlessBootstrap serverBootstrap = new ConnectionlessBootstrap(new OioDatagramChannelFactory(Executors.newCachedThreadPool()));
//        final syslogHandler handler = new syslogHandler();
//
//        serverBootstrap.setOption("receiveBufferSizePredictorFactory", 
//                        new AdaptiveReceiveBufferSizePredictorFactory(DEFAULT_MIN_SIZE, DEFAULT_INITIAL_SIZE, maxsize));
//
//        serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
//            @Override
//            public ChannelPipeline getPipeline() {
//                return Channels.pipeline(handler);
//            }
//        });
//
//        if (host == null) {
//            nettyChannel = serverBootstrap.bind(new InetSocketAddress(port));
//        } else {
//            nettyChannel = serverBootstrap.bind(new InetSocketAddress(host, port));
//        }
//
//        super.start();
//    }
//
//    @Override
//    public void stop() {
//        logger.info("Syslog UDP Source stopping...");
//        logger.info("Metrics:{}", counterGroup);
//
//        if (nettyChannel != null) {
//            nettyChannel.close();
//            try {
//                nettyChannel.getCloseFuture().await(60, TimeUnit.SECONDS);
//            } catch (InterruptedException e) {
//                logger.warn("netty server stop interrupted", e);
//            } finally {
//                nettyChannel = null;
//            }
//
//        }
//
//        super.stop();
//    }
//
//    @Override
//    public void configure(Context context) {
//        Configurables.ensureRequiredNonNull(context, SyslogSourceConfigurationConstants.CONFIG_PORT);
//        port = context.getInteger(SyslogSourceConfigurationConstants.CONFIG_PORT);
//        host = context.getString(SyslogSourceConfigurationConstants.CONFIG_HOST);                
//    }
//
//    @VisibleForTesting
//    public int getSourcePort() {
//        SocketAddress localAddress = nettyChannel.getLocalAddress();
//        if (localAddress instanceof InetSocketAddress) {
//            InetSocketAddress addr = (InetSocketAddress) localAddress;
//            return addr.getPort();
//        }
//        return 0;
//    }
//
}


//# Name the components on this agent
//a1.sources = r1
//a1.sinks = k1
//a1.channels = c1
//
//# Describe/configure the source
//a1.sources.r1.type = com.setfive.flume.UDPTextSource
//a1.sources.r1.bind = 0.0.0.0
//a1.sources.r1.port = 44444
//
//# Describe the sink
//a1.sinks.k1.type = logger
//
//# Use a channel which buffers events in memory
//a1.channels.c1.type = memory
//a1.channels.c1.capacity = 1000
//a1.channels.c1.transactionCapacity = 100
//
//# Bind the source and sink to the channel
//a1.sources.r1.channels = c1
//a1.sinks.k1.channel = c1

//ashish@ashish:~/Downloads/apache-flume-1.6.0-bin$ bin/flume-ng agent --conf conf --conf-file agent1.conf --name a1 -Dflume.root.logger=INFO,console

//ashish@ashish:~/Downloads$ echo "hi flume" | nc -4u -w1 localhost 44444