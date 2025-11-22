package com.github.euee.flink.cassandra;

import org.apache.flink.api.connector.source.SourceSplit;

/** Represents a split of Cassandra data based on token ranges. */
public class CassandraSourceSplit implements SourceSplit {

  private final String splitId;
  private final long startToken;
  private final long endToken;

  public CassandraSourceSplit(String splitId, long startToken, long endToken) {
    this.splitId = splitId;
    this.startToken = startToken;
    this.endToken = endToken;
  }

  @Override
  public String splitId() {
    return splitId;
  }

  public long getStartToken() {
    return startToken;
  }

  public long getEndToken() {
    return endToken;
  }

  @Override
  public String toString() {
    return "CassandraSourceSplit{"
        + "splitId='"
        + splitId
        + '\''
        + ", startToken="
        + startToken
        + ", endToken="
        + endToken
        + '}';
  }
}
