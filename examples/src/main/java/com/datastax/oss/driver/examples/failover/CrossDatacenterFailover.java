/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.examples.failover;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.QueryConsistencyException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * This example illustrates how to implement a cross-datacenter failover strategy from application
 * code.
 *
 * <p>Starting with driver 4.10, cross-datacenter failover is also provided as a configuration
 * option for built-in load balancing policies. See <a
 * href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/load_balancing/">Load
 * balancing</a> in the manual.
 *
 * <p>This example demonstrates how to achieve the same effect in application code, which confers
 * more fained-grained control over which statements should be retried and where.
 *
 * <p>The logic that decides whether or not a cross-DC failover should be attempted is presented in
 * the {@link #shouldFailover(DriverException)} method below; study it carefully and adapt it to
 * your needs if necessary.
 *
 * <p>The actual request execution and failover code is presented in 2 different programming styles:
 *
 * <ol>
 *   <li>Synchronous: see the {@link #writeSync(Statement)} method below;
 *   <li>Asynchronous: see the {@link #writeAsync(Statement)} method below;
 * </ol>
 * <p>
 * The 2 styles are identical in terms of failover effect; they are all included merely to help
 * programmers pick the variant that is closest to the style they use.
 *
 * <p>Preconditions:
 *
 * <ul>
 *   <li>An Apache Cassandra(R) cluster with two datacenters, dc1 and dc2, containing at least 3
 *       nodes in each datacenter, is running and accessible through the contact point:
 *       127.0.0.1:9042.
 * </ul>
 *
 * <p>Side effects:
 *
 * <ol>
 *   <li>Creates a new table {@code testks.orders}. If a table with that name exists already, it
 *       will be reused;
 *   <li>Tries to write a row in the table using the local datacenter dc1;
 *   <li>If the local datacenter dc1 is down, retries the write in the remote datacenter dc2.
 * </ol>
 *
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/latest">Java Driver online
 * manual</a>
 */
public class CrossDatacenterFailover {

  public static void main(String[] args) throws Exception {

    CrossDatacenterFailover client = new CrossDatacenterFailover();

    try {

      // Note: when this example is executed, at least the local DC must be available
      // since the driver will try to reach contact points in that DC.

      client.createConfig();
      client.connect();
      client.createSchema();

      // To fully exercise this example, try to stop the entire dc1 here; then observe how
      // the writes executed below will first fail in dc1, then be diverted to dc2, where they will
      // succeed.

      Statement<?> statement = SimpleStatement.newInstance(
              "INSERT INTO testks.orders "
                      + "(product_id, timestamp, price) "
                      + "VALUES ("
                      + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                      + "'2018-02-26T13:53:46.345+01:00',"
                      + "2.34)");

      long numberOfWrites = Long.parseLong(client.config.get("num_writes"));
      for (long i = 0; i < numberOfWrites; i++) {

        //client.writeSync(statement);
        client.writeAsync(statement);
      }

    } catch (Throwable e) {
      client.logger.error("Unknown error occurred", e);
    } finally {
      client.close();
    }
  }

  private Logger logger;
  private CqlSession primarySession;
  private CqlSession secondarySession;
  private Map<String, String> config;

  private CrossDatacenterFailover() {
    logger = LoggerFactory.getLogger(CrossDatacenterFailover.class);
  }

  private void createConfig() {

    config = new HashMap<>();
    config.put("primarySCB", System.getProperty("primarySCB", null));
    config.put("secondarySCB", System.getProperty("secondarySCB", null));
    config.put("keyspace", System.getProperty("keyspace", null));
    config.put("user", System.getProperty("user", null));
    config.put("password", System.getProperty("password", null));
    // For testing
    config.put("num_writes", System.getProperty("num_writes", "1"));

    StringBuilder missing = new StringBuilder();
    for (Map.Entry<String, String> entry : config.entrySet()) {
      if (entry.getValue() == null) {
        missing.append("-D").append(entry.getKey()).append(", ");
      }
    }

    if (missing.length() > 0) {
      System.out.println("The following properties are missing: " + missing);
      System.exit(1);
    }
  }

  private CqlSession createSession(String scb) {
    return CqlSession.builder()
            .withCloudSecureConnectBundle(Paths.get(scb))
            .withAuthCredentials(config.get("user"), config.get("password"))
            .withKeyspace(config.get("keyspace"))
            .build();
  }

  /**
   * Initiates a connection to the cluster.
   */
  private void connect() {

    try {

      primarySession = createSession(config.get("primarySCB"));
      String msg = "Connected to cluster with session: " + primarySession.getName();
      logger.info(msg);
      //System.out.println(msg);

    } catch (AllNodesFailedException | IllegalStateException e) {

      String msg = "Error connecting to primary session: " + e;
      logger.error(msg);
      //System.out.println(msg);
      primarySession = null;
    }

    try {

      secondarySession = createSession(config.get("secondarySCB"));
      String msg = "Connected to cluster with session: " + secondarySession.getName();
      logger.info(msg);
      //System.out.println(msg);

    } catch (AllNodesFailedException | IllegalStateException e) {

      String msg = "Error connecting to secondary session: " + e;
      logger.error(msg);
      //System.out.println(msg);
      secondarySession = null;
    }

    if (primarySession == null && secondarySession == null) {

      String msg = "Unable to connect to any session, exiting...";
      logger.error(msg);
      //System.out.println(msg);
      System.exit(1);
    }
  }

  /**
   * Creates the schema (keyspace) and table for this example.
   */
  private void createSchema() {

    String statement = "CREATE TABLE IF NOT EXISTS testks.orders ("
            + "product_id uuid,"
            + "timestamp timestamp,"
            + "price double,"
            + "PRIMARY KEY (product_id,timestamp)"
            + ")";

    try {

      primarySession.execute(statement);

    } catch (Exception e) {

      String msgPrimary = "Error creating schema, retrying with remote DC";
      logger.warn(msgPrimary);
      //System.out.println(msgPrimary);

      try {

        secondarySession.execute(statement);

      } catch (Exception e2) {

        String msgSecondary = "Error creating schema in remote DC";
        logger.error(msgSecondary, e2);
        //System.out.println(msgSecondary);
        //e2.printStackTrace();
      }

    }
  }

  /**
   * Inserts data synchronously using the local DC, retrying if necessary in a remote DC.
   */
  private void writeSync(Statement<?> statement) {

    //System.out.println("------- DC failover (sync) ------- ");

    try {

      // try the statement using the default profile, which targets the local datacenter.
      primarySession.execute(statement);

      String msgLocalSuccess = "Write to local DC succeeded";
      logger.debug(msgLocalSuccess);
      //System.out.println(msgLocalSuccess);

    } catch (DriverException e) {

      if (shouldFailover(e)) {

        String msgLocalFail = "Write failed in local DC, retrying in remote DC";
        logger.warn(msgLocalFail);
        //System.out.println(msgLocalFail);

        try {

          // try the statement using the secondary session, which targets the remote datacenter.
          secondarySession.execute(statement);

          String msgRemoteSuccess = "Write to remote DC succeeded";
          logger.debug(msgRemoteSuccess);
          //System.out.println(msgRemoteSuccess);

        } catch (DriverException e2) {

          String msgRemoteFail = "Write failed in remote DC";
          logger.error(msgRemoteFail, e2);
          //System.out.println(msgRemoteFail);
          //e2.printStackTrace();
        }
      }
    }
    // let other errors propagate
  }

  /**
   * Inserts data asynchronously using the local DC, retrying if necessary in a remote DC.
   */
  private void writeAsync(Statement<?> statement) throws Throwable {

    //System.out.println("------- DC failover (async) ------- ");

    CompletionStage<AsyncResultSet> result;

    if (primarySession != null) {
      result =
              // try the statement using the primary session, which targets the local datacenter.
              primarySession
                      .executeAsync(statement)
                      .handle(
                              (rs, error) -> {
                                if (error == null) {
                                  return CompletableFuture.completedFuture(rs);
                                } else {
                                  if (error instanceof DriverException
                                          && shouldFailover((DriverException) error)) {
                                    String msg = "Write failed in local DC, retrying in remote DC";
                                    logger.warn(msg);
                                    //System.out.println(msg);
                                    // try the statement using the secondary session, which targets the remote
                                    // datacenter.
                                    return secondarySession.executeAsync(statement);
                                  }
                                  // let other errors propagate
                                  return CompletableFutures.<AsyncResultSet>failedFuture(error);
                                }
                              })
                      // unwrap (flatmap) the nested future
                      .thenCompose(future -> future)
                      .whenComplete(
                              (rs, error) -> {
                                if (error == null) {
                                  String msg = "Write succeeded";
                                  logger.debug(msg);
                                  //System.out.println(msg);
                                } else {
                                  String msg = "Write failed";
                                  logger.error(msg, error);
                                  //System.out.println(msg);
                                  //error.printStackTrace();
                                }
                              });
    } else {
      result =
              secondarySession
                      .executeAsync(statement)
                      .whenComplete(
                              (rs, error) -> {
                                if (error == null) {
                                  String msg = "Write succeeded";
                                  logger.debug(msg);
                                  //System.out.println(msg);
                                } else {
                                  String msg = "Primary session is null, write failed in remote DC";
                                  logger.error(msg, error);
                                  //System.out.println(msg);
                                  //error.printStackTrace();
                                }
                              });
    }

    // for the sake of this example, wait for the operation to finish
    try {
      result.toCompletableFuture().get();
    } catch (Exception e) {
      // Any other kind of exception is logged immediately (and no other action is taken)
      String msg = "Unexpected exception occurred when retrieving future: " + e;
      logger.error(msg, e);
      //System.out.println(msg);
    }
  }

  /**
   * Analyzes the error and decides whether to failover to a remote DC.
   *
   * <p>The logic below categorizes driver exceptions in four main groups:
   *
   * <ol>
   *   <li>Total DC outage: all nodes in DC were known to be down when the request was executed;
   *   <li>Partial DC outage: one or many nodes responded, but reported a replica availability
   *       problem;
   *   <li>DC unreachable: one or many nodes were queried, but none responded (timeout);
   *   <li>Other errors.
   * </ol>
   * <p>
   * A DC failover is authorized for the first three groups above: total DC outage, partial DC
   * outage, and DC unreachable.
   *
   * <p>This logic is provided as a good starting point for users to create their own DC failover
   * strategy; please adjust it to your exact needs.
   */
  private boolean shouldFailover(DriverException mainException) {

    if (mainException instanceof NoNodeAvailableException) {

      // No node could be tried, because all nodes in the query plan were down. This could be a
      // total DC outage, so trying another DC makes sense.
      String msg = "All nodes were down in this datacenter, failing over";
      logger.warn(msg);
      //System.out.println(msg);
      return true;

    } else if (mainException instanceof AllNodesFailedException) {

      // Many nodes were tried (as decided by the retry policy), but all failed. This could be a
      // partial DC outage: some nodes were up, but the replicas were down.

      boolean failover = false;

      // Inspect the error to find out how many coordinators were tried, and which errors they
      // returned.
      for (Entry<Node, List<Throwable>> entry :
              ((AllNodesFailedException) mainException).getAllErrors().entrySet()) {

        Node coordinator = entry.getKey();
        List<Throwable> errors = entry.getValue();

        String msg = String.format("Node %s in DC %s was tried %d times but failed with:%n",
                coordinator.getEndPoint(), coordinator.getDatacenter(), errors.size());
        logger.warn(msg);
        //System.out.println(msg);

        for (Throwable nodeException : errors) {

          String msgNodeException = String.format("\t- %s%n", nodeException);
          logger.error(msgNodeException);
          //System.out.println(msgNodeException);

          // If the error was a replica availability error, then we know that some replicas were
          // down in this DC. Retrying in another DC could solve the problem. Other errors don't
          // necessarily mean that the DC is unavailable, so we ignore them.
          if (isReplicaAvailabilityError(nodeException)) {
            failover = true;
          }
        }
      }

      // Authorize the failover if at least one of the coordinators reported a replica availability
      // error that could be solved by trying another DC.
      if (failover) {
        String msg = "Some nodes tried in this DC reported a replica availability error, failing over";
        logger.warn(msg);
        //System.out.println(msg);
      } else {
        String msg = "All nodes tried in this DC failed unexpectedly, not failing over";
        logger.error(msg);
        //System.out.println(msg);
      }
      return failover;

    } else if (mainException instanceof DriverTimeoutException) {

      // One or many nodes were tried, but none replied in a timely manner, and the timeout defined
      // by the option `datastax-java-driver.basic.request.timeout` was triggered.
      // This could be a DC outage as well, or a network partition issue, so trying another DC may
      // make sense.
      // Note about SLAs: if your application needs to comply with SLAs, and the maximum acceptable
      // latency for a request is equal or very close to the request timeout, beware that failing
      // over to a different datacenter here could potentially break your SLA.

      String msg = "No node in this DC replied before the timeout was triggered, failing over";
      logger.warn(msg);
      //System.out.println(msg);
      return true;

    } else if (mainException instanceof CoordinatorException) {

      // Only one node was tried, and it failed (and the retry policy did not tell the driver to
      // retry this request, but rather to surface the error immediately). This is rather unusual
      // as the driver's default retry policy retries most of these errors, but some custom retry
      // policies could decide otherwise. So we apply the same logic as above: if the error is a
      // replica availability error, we authorize the failover.

      Node coordinator = ((CoordinatorException) mainException).getCoordinator();
      String msg = String.format("Node %s in DC %s was tried once but failed with: %s%n",
              coordinator.getEndPoint(), coordinator.getDatacenter(), mainException);
      logger.error(msg);
      //System.out.println(msg);

      boolean failover = isReplicaAvailabilityError(mainException);
      if (failover) {
        String msgFailover = "The only node tried in this DC reported a replica availability error, failing over";
        logger.warn(msgFailover);
        //System.out.println(msgFailover);
      } else {
        String msgNoFailover = "The only node tried in this DC failed unexpectedly, not failing over";
        logger.error(msgNoFailover);
        //System.out.println(msgNoFailover);
      }
      return failover;

    } else {

      // The request failed with a rather unusual error. This generally indicates a more serious
      // issue, since the retry policy decided to surface the error immediately. Trying another DC
      // is probably a bad idea.
      String msg = "The request failed unexpectedly, not failing over: " + mainException;
      logger.error(msg, mainException);
      //System.out.println(msg);
      return false;
    }
  }

  /**
   * Whether the given error is a replica availability error.
   *
   * <p>A replica availability error means that the initial consistency level could not be met
   * because not enough replicas were alive.
   *
   * <p>When this error happens, it can be worth failing over to a remote DC, <em>as long as at
   * least one of the following conditions apply</em>:
   *
   * <ol>
   *   <li>if the initial consistency level was DC-local, trying another DC may succeed;
   *   <li>if the initial consistency level can be downgraded, then retrying again may succeed (in
   *       the same DC, or in another one).
   * </ol>
   * <p>
   * In this example both conditions above apply, so we authorize the failover whenever we detect a
   * replica availability error.
   */
  private boolean isReplicaAvailabilityError(Throwable t) {
    return t instanceof UnavailableException || t instanceof QueryConsistencyException;
  }

  private void close() {
    if (primarySession != null) {
      primarySession.close();
    }

    if (secondarySession != null) {
      secondarySession.close();
    }
  }
}
