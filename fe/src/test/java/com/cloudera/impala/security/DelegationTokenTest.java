// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.security.ZooKeeperTokenStore.TokenStoreException;
import com.cloudera.impala.service.ZooKeeperSession;

// TODO: test expiration.
public class DelegationTokenTest {
  // Connection to the local ZK running and a non-secure ACL.
  public static final String ZOOKEEPER_HOSTPORT = "localhost:2181";
  public static final String ZOOKEEPER_ACL = "world:anyone:cdrwa";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Setup log4j for testing.
    org.apache.log4j.BasicConfigurator.configure();
  }

  // Basic tests to verify that tokens can be generated. This directly accesses
  // the imported code.
  @Test
  public void TestStartSecretManager() throws IOException {
    DelegationTokenSecretManager mgr = new DelegationTokenSecretManager(
        0, 60*60*1000, 60*60*1000, 0);
    mgr.startThreads();

    String userName = UserGroupInformation.getCurrentUser().getUserName();

    // Create a token for user.
    String tokenStrForm = mgr.getDelegationToken(userName);
    Token<DelegationTokenIdentifier> t = new Token<DelegationTokenIdentifier>();
    t.decodeFromUrlString(tokenStrForm);

    // Check the token contains the proper username.
    DelegationTokenIdentifier d = new DelegationTokenIdentifier();
    d.readFields(new DataInputStream(new ByteArrayInputStream(
        t.getIdentifier())));
    assertTrue("Usernames don't match", userName.equals(d.getUser().getShortUserName()));
    assertEquals(d.getSequenceNumber(), 1);

    byte[] password = mgr.retrievePassword(d);
    assertEquals(password.length, t.getPassword().length);
    for (int i = 0; i < t.getPassword().length; ++i) {
      assertEquals(t.getPassword()[i], password[i]);
    }

    mgr.stopThreads();
  }

  private void testTokenManager(boolean useZK) throws IOException {
    String userName = UserGroupInformation.getCurrentUser().getUserName();
    Configuration config = new Configuration();
    ZooKeeperSession zk = null;
    if (useZK) {
      config.set(ZooKeeperSession.ZOOKEEPER_CONNECTION_STRING_CONF,
          ZOOKEEPER_HOSTPORT);
      config.set(ZooKeeperSession.ZOOKEEPER_STORE_ACL_CONF,
          ZOOKEEPER_ACL);
      zk = new ZooKeeperSession(config, "test", 1, 1);
    }
    DelegationTokenManager mgr = new DelegationTokenManager(config, true, zk);

    // Create two tokens
    byte[] token1 = mgr.getToken(userName, userName, userName).token;
    byte[] token2 = mgr.getToken(userName, userName, null).token;

    // Retrieve the passwords by token. Although the token contains the
    // password, this retrieves it using just the identifier.
    byte[] password1 = mgr.getPasswordByToken(token1);
    byte[] password2 = mgr.getPasswordByToken(token2);

    // Make sure it matches the password in token and doesn't match the password for
    // the other token.
    Token<DelegationTokenIdentifier> t1 = new Token<DelegationTokenIdentifier>();
    t1.decodeFromUrlString(new String(token1));
    assertTrue(Arrays.equals(t1.getPassword(), password1));
    assertFalse(Arrays.equals(t1.getPassword(), password2));

    // Get the password from just the identifier. This does not contain the password
    // but the server stores it.
    DelegationTokenIdentifier id1 = new DelegationTokenIdentifier();
    id1.readFields(new DataInputStream(new ByteArrayInputStream(
        t1.getIdentifier())));
    byte[] serializedId1 = Base64.encodeBase64(id1.serialize());
    assertTrue(serializedId1.length < token1.length);

    // Retrieve the password from the manager by serialized id.
    DelegationTokenManager.UserPassword userPw =
        mgr.retrieveUserPassword(new String(serializedId1));
    assertTrue(Arrays.equals(password1, Base64.decodeBase64(userPw.password)));
    assertEquals(userName, userPw.user);

    // Cancel token2, token1 should continue to work fine.
    mgr.cancelToken(userName, token2);
    assertTrue(Arrays.equals(mgr.getPasswordByToken(token1), password1));

    // Renew token1, should continue to work.
    mgr.renewToken(userName, token1);
    assertTrue(Arrays.equals(mgr.getPasswordByToken(token1), password1));

    // Cancel token1, should fail to get password for it.
    mgr.cancelToken(userName, token1);
    boolean exceptionThrown = false;
    try {
      mgr.getPasswordByToken(token1);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be found"));
    } catch (TokenStoreException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains("Token does not exist"));
    }
    assertTrue(exceptionThrown);

    // Try to renew.
    exceptionThrown = false;
    try {
      mgr.renewToken(userName, token1);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Renewal request for unknown token"));
    } catch (TokenStoreException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains("Token does not exist"));
    }
    assertTrue(exceptionThrown);

    // Try to cancel.
    try {
      mgr.cancelToken(userName, token1);
    } catch (IOException e) {
      // Depending on the underlying store (ZK vs in mem), we will throw an exception
      // or silently fail. Having cancel be idempotent is reasonable and the ZK
      // behavior.
      assertTrue(e.getMessage().contains("Token not found"));
    }

    // Try a corrupt token.
    exceptionThrown = false;
    try {
      mgr.cancelToken(userName, new byte[100]);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Token is corrupt."));
    }
    assertTrue(exceptionThrown);
  }

  @Test
  public void TestTokenManager() throws IOException {
    testTokenManager(false);
    testTokenManager(true);
  }
}
