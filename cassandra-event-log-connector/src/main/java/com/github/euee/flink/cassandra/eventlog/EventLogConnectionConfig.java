package com.github.euee.flink.cassandra.eventlog;

import java.io.Serializable;

/** Configuration for Cassandra event log connection and polling behavior. */
public class EventLogConnectionConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String host;
  private final int port;
  private final String username;
  private final String password;
  private final String keyspace;
  private final String table;
  private final String datacenter;
  private final int numShards;
  private final long pollIntervalMs;
  private final int fetchSize;
  private final int lookbackHours;

  private EventLogConnectionConfig(Builder builder) {
    this.host = builder.host;
    this.port = builder.port;
    this.username = builder.username;
    this.password = builder.password;
    this.keyspace = builder.keyspace;
    this.table = builder.table;
    this.datacenter = builder.datacenter;
    this.numShards = builder.numShards;
    this.pollIntervalMs = builder.pollIntervalMs;
    this.fetchSize = builder.fetchSize;
    this.lookbackHours = builder.lookbackHours;
  }

  public static Builder builder() {
    return new Builder();
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

  public String getKeyspace() {
    return keyspace;
  }

  public String getTable() {
    return table;
  }

  public String getDatacenter() {
    return datacenter;
  }

  public int getNumShards() {
    return numShards;
  }

  public long getPollIntervalMs() {
    return pollIntervalMs;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public int getLookbackHours() {
    return lookbackHours;
  }

  /** Builder for EventLogConnectionConfig. */
  public static class Builder {
    private String host;
    private int port = 9042;
    private String username;
    private String password;
    private String keyspace;
    private String table;
    private String datacenter = "datacenter1";
    private int numShards = 16;
    private long pollIntervalMs = 1000L;
    private int fetchSize = 5000;
    private int lookbackHours = 1;

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder username(String username) {
      this.username = username;
      return this;
    }

    public Builder password(String password) {
      this.password = password;
      return this;
    }

    public Builder keyspace(String keyspace) {
      this.keyspace = keyspace;
      return this;
    }

    public Builder table(String table) {
      this.table = table;
      return this;
    }

    public Builder datacenter(String datacenter) {
      this.datacenter = datacenter;
      return this;
    }

    public Builder numShards(int numShards) {
      this.numShards = numShards;
      return this;
    }

    public Builder pollIntervalMs(long pollIntervalMs) {
      this.pollIntervalMs = pollIntervalMs;
      return this;
    }

    public Builder fetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }

    public Builder lookbackHours(int lookbackHours) {
      this.lookbackHours = lookbackHours;
      return this;
    }

    public EventLogConnectionConfig build() {
      return new EventLogConnectionConfig(this);
    }
  }
}
