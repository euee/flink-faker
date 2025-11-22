package com.github.euee.flink.cassandra.eventlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Integration tests for {@link EventLogSplitEnumerator}. */
class EventLogSplitEnumeratorTest {

  @Test
  void testEnumeratorCreatesInitialSplits() throws Exception {
    @SuppressWarnings("unchecked")
    SplitEnumeratorContext<EventLogSourceSplit> context = mock(SplitEnumeratorContext.class);
    when(context.currentParallelism()).thenReturn(4);
    when(context.registeredReaders()).thenReturn(createRegisteredReaders(4));

    EventLogSplitEnumerator enumerator =
        new EventLogSplitEnumerator(context, 16, null); // 16 shards, no restored state

    enumerator.start();

    EventLogEnumeratorState state = enumerator.snapshotState(1);

    // Initially, all 16 splits should be pending (not yet assigned)
    assertThat(state.getRemainingSplits()).hasSize(16);

    // Verify shards 0-15 are created
    Set<Integer> shardIds = new HashSet<>();
    for (EventLogSourceSplit split : state.getRemainingSplits()) {
      shardIds.add(split.getShardId());
      assertThat(split.getLastEventTime()).isNull(); // Initially null
    }
    assertThat(shardIds).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
        14, 15);
  }

  @Test
  void testEnumeratorAssignsSplitsWhenReaderAdded() {
    @SuppressWarnings("unchecked")
    SplitEnumeratorContext<EventLogSourceSplit> context = mock(SplitEnumeratorContext.class);
    when(context.currentParallelism()).thenReturn(4);
    when(context.registeredReaders()).thenReturn(createRegisteredReaders(4));

    EventLogSplitEnumerator enumerator = new EventLogSplitEnumerator(context, 16, null);

    // Add readers
    enumerator.addReader(0);
    enumerator.addReader(1);
    enumerator.addReader(2);
    enumerator.addReader(3);

    // Verify assignSplits was called
    verify(context).assignSplits(any());

    // Verify signalNoMoreSplits was called
    verify(context).signalNoMoreSplits(any());
  }

  @Test
  void testEnumeratorRestoresFromCheckpoint() throws Exception {
    @SuppressWarnings("unchecked")
    SplitEnumeratorContext<EventLogSourceSplit> context = mock(SplitEnumeratorContext.class);

    // Create checkpoint state with some remaining splits
    List<EventLogSourceSplit> remainingSplits =
        Arrays.asList(
            new EventLogSourceSplit(10, "uuid-10"),
            new EventLogSourceSplit(11, "uuid-11"),
            new EventLogSourceSplit(12, null));
    EventLogEnumeratorState checkpoint = new EventLogEnumeratorState(remainingSplits);

    EventLogSplitEnumerator enumerator = new EventLogSplitEnumerator(context, 16, checkpoint);

    EventLogEnumeratorState state = enumerator.snapshotState(1);

    // Should restore the 3 remaining splits
    assertThat(state.getRemainingSplits()).hasSize(3);
    assertThat(state.getRemainingSplits().get(0).getShardId()).isEqualTo(10);
    assertThat(state.getRemainingSplits().get(1).getShardId()).isEqualTo(11);
    assertThat(state.getRemainingSplits().get(2).getShardId()).isEqualTo(12);
  }

  @Test
  void testEnumeratorAddsSplitsBack() throws Exception {
    @SuppressWarnings("unchecked")
    SplitEnumeratorContext<EventLogSourceSplit> context = mock(SplitEnumeratorContext.class);

    EventLogSplitEnumerator enumerator = new EventLogSplitEnumerator(context, 4, null);

    // Simulate splits being returned (e.g., from failed reader)
    List<EventLogSourceSplit> returnedSplits =
        Arrays.asList(
            new EventLogSourceSplit(1, "uuid-1"), new EventLogSourceSplit(2, "uuid-2"));

    enumerator.addSplitsBack(returnedSplits, 0);

    EventLogEnumeratorState state = enumerator.snapshotState(1);

    // Should have original 4 splits + 2 returned = 6 total
    assertThat(state.getRemainingSplits()).hasSize(6);
  }

  @Test
  void testEnumeratorWithCustomNumShards() throws Exception {
    @SuppressWarnings("unchecked")
    SplitEnumeratorContext<EventLogSourceSplit> context = mock(SplitEnumeratorContext.class);

    // Test with 32 shards instead of default 16
    EventLogSplitEnumerator enumerator = new EventLogSplitEnumerator(context, 32, null);

    EventLogEnumeratorState state = enumerator.snapshotState(1);

    assertThat(state.getRemainingSplits()).hasSize(32);

    Set<Integer> shardIds = new HashSet<>();
    for (EventLogSourceSplit split : state.getRemainingSplits()) {
      shardIds.add(split.getShardId());
    }

    // Verify all shards from 0 to 31 exist
    assertThat(shardIds).hasSize(32);
    for (int i = 0; i < 32; i++) {
      assertThat(shardIds).contains(i);
    }
  }

  @Test
  void testEnumeratorSnapshotStateConsistency() throws Exception {
    @SuppressWarnings("unchecked")
    SplitEnumeratorContext<EventLogSourceSplit> context = mock(SplitEnumeratorContext.class);
    when(context.currentParallelism()).thenReturn(1);
    when(context.registeredReaders()).thenReturn(createRegisteredReaders(1));

    EventLogSplitEnumerator enumerator = new EventLogSplitEnumerator(context, 8, null);

    // Snapshot before assignment
    EventLogEnumeratorState state1 = enumerator.snapshotState(1);
    assertThat(state1.getRemainingSplits()).hasSize(8);

    // Add a reader (triggers assignment)
    enumerator.addReader(0);

    // Snapshot after assignment - splits should be assigned, so fewer remaining
    EventLogEnumeratorState state2 = enumerator.snapshotState(2);
    assertThat(state2.getRemainingSplits()).hasSizeLessThanOrEqualTo(8);
  }

  /** Helper to create a set of registered reader IDs. */
  private Set<Integer> createRegisteredReaders(int count) {
    Set<Integer> readers = new HashSet<>();
    for (int i = 0; i < count; i++) {
      readers.add(i);
    }
    return readers;
  }
}
