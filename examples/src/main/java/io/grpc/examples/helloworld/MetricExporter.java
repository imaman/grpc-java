package io.grpc.examples.helloworld;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class MetricExporter {
  private static final String API_KEY = "b1696984-3968-440e-bb3b-ab1f0989d51a";
  private final Socket conn;
  private final DataOutputStream dos;
  private final Map<String, Long> countersByName = new HashMap<>();
  
  public MetricExporter() throws Exception {
    conn = new Socket("7d389953.carbon.hostedgraphite.com", 2003);
    dos = new DataOutputStream(conn.getOutputStream());
  }

  public synchronized void increment(String name, long amount) throws IOException {
    Long n = countersByName.get(name);
    if (n == null) {
      n = 0L;
    }
    
    n += amount;
    countersByName.put(name, n);
    dos.writeBytes(String.format("%s.%s %s\n", API_KEY, name, n));
  }
}
