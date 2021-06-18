/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.types.Password;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import kafka.server.KafkaConfig;
import uk.org.webcompere.systemstubs.rules.EnvironmentVariablesRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class EnvConfigProviderTest {

  public static final String TEST_PASSWORD = "testPassword123";
  public static final String NOT_SUBSTITUTED_CONFIG = "${env:SSL_KEY_PASSWORD}";
  public static final String ENV_CONFIG_PROVIDER_TEST_PROPERTIES = "envConfigProviderTest.properties";
  private static final List<String> BOOTSTRAP_SERVERS_LIST = Arrays.asList("localhost:9092");
  private static final String ZOOKEEPER_CONNECT = "localhost:2181";

  @Rule
  public EnvironmentVariablesRule _environmentVariablesRule = new EnvironmentVariablesRule();

  @Test
  public void testEnvConfigProvider() throws IOException {
    KafkaCruiseControlConfig configs = KafkaCruiseControlUtils.readConfig(
        Objects.requireNonNull(this.getClass().getClassLoader().getResource(ENV_CONFIG_PROVIDER_TEST_PROPERTIES)).getPath());

    // Test password substitution
    Password actualSslKeystorePassword = configs.getPassword(WebServerConfig.WEBSERVER_SSL_KEYSTORE_PASSWORD_CONFIG);
    Password expectedSslKeystorePassword = new Password(TEST_PASSWORD);
    assertEquals(expectedSslKeystorePassword, actualSslKeystorePassword);

    // Test when the environment variable is not defined and the password isn't substituted
    Password actualSslKeyPassword = configs.getPassword(WebServerConfig.WEBSERVER_SSL_KEY_PASSWORD_CONFIG);
    Password expectedSslKeyPassword = new Password(NOT_SUBSTITUTED_CONFIG);
    assertEquals(expectedSslKeyPassword, actualSslKeyPassword);

    // Test for bootstrap.servers and zk.connect
    List<String> actualServersList = configs.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
    assertEquals(actualServersList.toString(), BOOTSTRAP_SERVERS_LIST.toString());
    String zookeeperList = configs.getString(KafkaConfig.ZkConnectProp());
    assertEquals(zookeeperList, ZOOKEEPER_CONNECT);
  }

  /**
   * Setup environment variables for kafka broker connect and zk connect
   */
  @Before
  public void setupEnvironmentVariables() {
    _environmentVariablesRule.set("KAFKA_BROKERS", "localhost:9092");
    _environmentVariablesRule.set("ZK_CONNECT", "localhost:2181");
  }

}
