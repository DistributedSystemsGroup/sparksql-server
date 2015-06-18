/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class RpcIntegrationSuite {
  static TransportServer server;
  static TransportClientFactory clientFactory;
  static RpcHandler rpcHandler;

  @BeforeClass
  public static void setUp() throws Exception {
    TransportConf conf = new TransportConf(new SystemPropertyConfigProvider());
    rpcHandler = new RpcHandler() {
      @Override
      public void receive(TransportClient client, byte[] message, RpcResponseCallback callback) {
        String msg = new String(message, Charsets.UTF_8);
        String[] parts = msg.split("/");
        if (parts[0].equals("hello")) {
          callback.onSuccess(("Hello, " + parts[1] + "!").getBytes(Charsets.UTF_8));
        } else if (parts[0].equals("return error")) {
          callback.onFailure(new RuntimeException("Returned: " + parts[1]));
        } else if (parts[0].equals("throw error")) {
          throw new RuntimeException("Thrown: " + parts[1]);
        }
      }

      @Override
      public StreamManager getStreamManager() { return new OneForOneStreamManager(); }
    };
    TransportContext context = new TransportContext(conf, rpcHandler);
    server = context.createServer();
    clientFactory = context.createClientFactory();
  }

  @AfterClass
  public static void tearDown() {
    server.close();
    clientFactory.close();
  }

  class RpcResult {
    public Set<String> successMessages;
    public Set<String> errorMessages;
  }

  private RpcResult sendRPC(String ... commands) throws Exception {
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    final Semaphore sem = new Semaphore(0);

    final RpcResult res = new RpcResult();
    res.successMessages = Collections.synchronizedSet(new HashSet<String>());
    res.errorMessages = Collections.synchronizedSet(new HashSet<String>());

    RpcResponseCallback callback = new RpcResponseCallback() {
      @Override
      public void onSuccess(byte[] message) {
        res.successMessages.add(new String(message, Charsets.UTF_8));
        sem.release();
      }

      @Override
      public void onFailure(Throwable e) {
        res.errorMessages.add(e.getMessage());
        sem.release();
      }
    };

    for (String command : commands) {
      client.sendRpc(command.getBytes(Charsets.UTF_8), callback);
    }

    if (!sem.tryAcquire(commands.length, 5, TimeUnit.SECONDS)) {
      fail("Timeout getting response from the server");
    }
    client.close();
    return res;
  }

  @Test
  public void singleRPC() throws Exception {
    RpcResult res = sendRPC("hello/Aaron");
    assertEquals(res.successMessages, Sets.newHashSet("Hello, Aaron!"));
    assertTrue(res.errorMessages.isEmpty());
  }

  @Test
  public void doubleRPC() throws Exception {
    RpcResult res = sendRPC("hello/Aaron", "hello/Reynold");
    assertEquals(res.successMessages, Sets.newHashSet("Hello, Aaron!", "Hello, Reynold!"));
    assertTrue(res.errorMessages.isEmpty());
  }

  @Test
  public void returnErrorRPC() throws Exception {
    RpcResult res = sendRPC("return error/OK");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Returned: OK"));
  }

  @Test
  public void throwErrorRPC() throws Exception {
    RpcResult res = sendRPC("throw error/uh-oh");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Thrown: uh-oh"));
  }

  @Test
  public void doubleTrouble() throws Exception {
    RpcResult res = sendRPC("return error/OK", "throw error/uh-oh");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Returned: OK", "Thrown: uh-oh"));
  }

  @Test
  public void sendSuccessAndFailure() throws Exception {
    RpcResult res = sendRPC("hello/Bob", "throw error/the", "hello/Builder", "return error/!");
    assertEquals(res.successMessages, Sets.newHashSet("Hello, Bob!", "Hello, Builder!"));
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Thrown: the", "Returned: !"));
  }

  private void assertErrorsContain(Set<String> errors, Set<String> contains) {
    assertEquals(contains.size(), errors.size());

    Set<String> remainingErrors = Sets.newHashSet(errors);
    for (String contain : contains) {
      Iterator<String> it = remainingErrors.iterator();
      boolean foundMatch = false;
      while (it.hasNext()) {
        if (it.next().contains(contain)) {
          it.remove();
          foundMatch = true;
          break;
        }
      }
      assertTrue("Could not find error containing " + contain + "; errors: " + errors, foundMatch);
    }

    assertTrue(remainingErrors.isEmpty());
  }
}
