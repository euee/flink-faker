package com.github.euee.flink.cassandra;

import java.io.Serializable;

/** Configuration for Cassandra connection. */
public class CassandraConnectionConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String host;
  private final int port;
  private final String username;
  private final String password;
  private final String datacenter;

  public CassandraConnectionConfig(
      String host, int port, String username, String password, String datacenter) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.datacenter = datacenter;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getDatacenter() {
    return datacenter;
  }
}
