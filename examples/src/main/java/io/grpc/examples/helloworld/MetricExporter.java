package io.grpc.examples.helloworld;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * Sends metrics to HostedGraphite. 
 * Dashboard: {@link https://www.hostedgraphite.com/7d389953/grafana/dashboard/db/cs_236700}
 */
public class MetricExporter {
  private static final String API_KEY = "b1696984-3968-440e-bb3b-ab1f0989d51a";
  private final Socket conn;
  private final DataOutputStream dos;
  private final Map<String, Long> countersByName = new HashMap<>();
  
  public MetricExporter() {
    try {
      conn = new Socket("7d389953.carbon.hostedgraphite.com", 2003);
      dos = new DataOutputStream(conn.getOutputStream());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void increment(String name, long amount) {
    Long n = countersByName.get(name);
    if (n == null) {
      n = 0L;
    }
    
    n += amount;
    countersByName.put(name, n);
    try {
      dos.writeBytes(String.format("%s.%s %s\n", API_KEY, name, n));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
