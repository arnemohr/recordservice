package com.cloudera.impala.service;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

// TODO: add some multithreading tests.
public class ZooKeeperTest {
  // Connection to the local ZK running and a non-secure ACL.
  public static final String ZOOKEEPER_HOSTPORT = "localhost:2181";
  public static final String ZOOKEEPER_ACL = "world:anyone:cdrwa";

  // Verifies the membership is correct. This is async so retry and sleep a few times.
  private void verifyMembership(ZooKeeperSession session, boolean planner, int expected)
      throws InterruptedException {
    int lastSize = 0;
    for (int i = 0; i < 5; ++i) {
      Thread.sleep(1000);
      lastSize = session.getMembershipSize(planner);
      if (lastSize == expected) return;
    }
    assertEquals("Did not reach membership size.", lastSize, expected);
  }

  @Test
  public void testMembership() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.set(ZooKeeperSession.ZOOKEEPER_CONNECTION_STRING_CONF, ZOOKEEPER_HOSTPORT);
    conf.set(ZooKeeperSession.ZOOKEEPER_STORE_ACL_CONF, ZOOKEEPER_ACL);

    // Start a session that runs the planner and worker.
    ZooKeeperSession session1 = new ZooKeeperSession(conf, "s1", 1, 1);
    // Should see one planner.
    verifyMembership(session1, true, 1);

    // Should see one worker.
    verifyMembership(session1, false, 1);

    // Start a session that just runs a worker.
    ZooKeeperSession session2 = new ZooKeeperSession(conf, "s2", 0, 1);
    Thread.sleep(5000);

    // Should still see one planner.
    verifyMembership(session1, true, 1);
    // Should see two workers.
    verifyMembership(session1, false, 2);

    final int NUM_SESSIONS = 10;
    ZooKeeperSession[] sessions = new ZooKeeperSession[NUM_SESSIONS];
    for (int i = 0; i < sessions.length; ++i) {
      // Start up some more planners and workers.
      sessions[i] = new ZooKeeperSession(
        conf, "session_" + i, (i % 2) == 0 ? 1 : 0, 1);
    }

    verifyMembership(session1, true, 1 + NUM_SESSIONS / 2);
    verifyMembership(session1, false, 2 + NUM_SESSIONS);

    for (int i = 0; i < sessions.length; ++i) {
      if (i % 2 == 0) {
        // This is a planner.
        verifyMembership(sessions[i], true, 1 + NUM_SESSIONS / 2);
        verifyMembership(sessions[i], false, 2 + NUM_SESSIONS);
      }
    }

    // Close these additional sessions.
    for (int i = 0; i < sessions.length; ++i) {
      sessions[i].close();
    }

    // Number of planners should drop down to 1.
    verifyMembership(session1, true, 1);

    // Number of workers should drop down to 2.
    verifyMembership(session1, false, 2);

    // Close 2.
    session2.close();
    verifyMembership(session1, true, 1);
    verifyMembership(session1, false, 1);

    // Use session2, this should make it reconnect.
    session2.getSession();
    // Should be able to see it again.
    verifyMembership(session1, true, 1);
    verifyMembership(session1, false, 2);

    session1.close();
    verifyMembership(session1, true, 0);
    verifyMembership(session1, false, 0);

    // Reconnect session1, should see both.
    session1.getSession();
    verifyMembership(session1, true, 1);
    verifyMembership(session1, false, 2);

    session1.close();
    session2.close();
  }

  @Test
  public void testMembershipMore() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.set(ZooKeeperSession.ZOOKEEPER_CONNECTION_STRING_CONF, ZOOKEEPER_HOSTPORT);
    conf.set(ZooKeeperSession.ZOOKEEPER_STORE_ACL_CONF, ZOOKEEPER_ACL);

    // Test the case when there are only planners
    List<ZooKeeperSession> sessions = new ArrayList<ZooKeeperSession>();
    for (int i = 0; i < 3; ++i) {
      sessions.add(new ZooKeeperSession(conf, "session_" + i, 1, 0));
    }
    for (int i = 0; i < sessions.size(); ++i) {
      verifyMembership(sessions.get(i), true, sessions.size());
    }
    for (int i = 0; i < sessions.size(); ++i) {
      sessions.get(i).close();
    }

    // Test the case when there are only workers
    sessions.clear();
    for (int i = 0; i < 3; ++i) {
      sessions.add(new ZooKeeperSession(conf, "session_" + i, 0, 1));
    }
    for (int i = 0; i < sessions.size(); ++i) {
      verifyMembership(sessions.get(i), false, sessions.size());
    }
    for (int i = 0; i < sessions.size(); ++i) {
      sessions.get(i).close();
    }
  }
}
