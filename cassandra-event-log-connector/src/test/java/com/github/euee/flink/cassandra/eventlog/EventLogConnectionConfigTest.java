package com.github.euee.flink.cassandra.eventlog;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Unit tests for {@link EventLogConnectionConfig}. */
class EventLogConnectionConfigTest {

  @Test
  void testBuilderWithAllFields() {
    EventLogConnectionConfig config =
        EventLogConnectionConfig.builder()
            .host("cassandra-host")
            .port(9042)
            .username("test-user")
            .password("test-password")
            .keyspace("test-keyspace")
            .table("test-table")
            .datacenter("dc1")
            .numShards(32)
            .pollIntervalMs(2000L)
            .fetchSize(10000)
            .lookbackHours(2)
            .build();

    assertThat(config.getHost()).isEqualTo("cassandra-host");
    assertThat(config.getPort()).isEqualTo(9042);
    assertThat(config.getUsername()).isEqualTo("test-user");
    assertThat(config.getPassword()).isEqualTo("test-password");
    assertThat(config.getKeyspace()).isEqualTo("test-keyspace");
    assertThat(config.getTable()).isEqualTo("test-table");
    assertThat(config.getDatacenter()).isEqualTo("dc1");
    assertThat(config.getNumShards()).isEqualTo(32);
    assertThat(config.getPollIntervalMs()).isEqualTo(2000L);
    assertThat(config.getFetchSize()).isEqualTo(10000);
    assertThat(config.getLookbackHours()).isEqualTo(2);
  }

  @Test
  void testBuilderWithDefaults() {
    EventLogConnectionConfig config =
        EventLogConnectionConfig.builder()
            .host("localhost")
            .username("cassandra")
            .password("cassandra")
            .keyspace("events")
            .table("event_log")
            .build();

    // Check defaults
    assertThat(config.getPort()).isEqualTo(9042);
    assertThat(config.getDatacenter()).isEqualTo("datacenter1");
    assertThat(config.getNumShards()).isEqualTo(16);
    assertThat(config.getPollIntervalMs()).isEqualTo(1000L);
    assertThat(config.getFetchSize()).isEqualTo(5000);
    assertThat(config.getLookbackHours()).isEqualTo(1);
  }

  @Test
  void testBuilderChaining() {
    EventLogConnectionConfig config =
        EventLogConnectionConfig.builder()
            .host("host1")
            .port(9043)
            .username("user1")
            .password("pass1")
            .keyspace("ks1")
            .table("table1")
            .datacenter("dc2")
            .numShards(8)
            .pollIntervalMs(500L)
            .fetchSize(1000)
            .lookbackHours(3)
            .build();

    assertThat(config).isNotNull();
    assertThat(config.getNumShards()).isEqualTo(8);
    assertThat(config.getPollIntervalMs()).isEqualTo(500L);
  }
}
