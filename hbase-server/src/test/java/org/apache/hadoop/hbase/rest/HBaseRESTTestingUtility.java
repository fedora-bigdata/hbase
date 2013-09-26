/**
 *
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
package org.apache.hadoop.hbase.rest;

import java.util.EnumSet;
import javax.servlet.DispatcherType;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.rest.filter.GzipFilter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.util.StringUtils;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.sun.jersey.spi.container.servlet.ServletContainer;

public class HBaseRESTTestingUtility {

  static final Log LOG = LogFactory.getLog(HBaseRESTTestingUtility.class);

  private int testServletPort;
  private Server server;

  public int getServletPort() {
    return testServletPort;
  }

  public void startServletContainer(Configuration conf) throws Exception {
    if (server != null) {
      LOG.error("ServletContainer already running");
      return;
    }

    // Inject the conf for the test by being first to make singleton
    RESTServlet.getInstance(conf, User.getCurrent().getUGI());

    // set up the Jersey servlet container for Jetty
    ServletHolder sh = new ServletHolder(ServletContainer.class);
    sh.setInitParameter(
      "com.sun.jersey.config.property.resourceConfigClass",
      ResourceConfig.class.getCanonicalName());
    sh.setInitParameter("com.sun.jersey.config.property.packages",
      "jetty");

    LOG.info("configured " + ServletContainer.class.getName());
    
    HttpConfiguration http_config = new HttpConfiguration();
    http_config.setSendServerVersion(false);
    http_config.setSendDateHeader(false);

    // set up Jetty and run the embedded server
    server = new Server(0);
      // set up context
    ServletContextHandler context = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
    context.addServlet(sh, "/*");
    // Load filters specified from configuration.
    String[] filterClasses = conf.getStrings(Constants.FILTER_CLASSES,
      ArrayUtils.EMPTY_STRING_ARRAY);
    for (String filter : filterClasses) {
      filter = filter.trim();
      context.addFilter(filter, "/*", EnumSet.of(DispatcherType.REQUEST));
    }
    LOG.info("Loaded filter classes :" + filterClasses);
    ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory(http_config));
    server.addConnector(connector);
      // start the server
    server.start();
      // get the port
    testServletPort = ((ServerConnector) server.getConnectors()[0]).getLocalPort();

    LOG.info("started " + server.getClass().getName() + " on port " + 
      testServletPort);
  }

  public void shutdownServletContainer() {
    if (server != null) try {
      server.stop();
      server = null;
      RESTServlet.stop();
    } catch (Exception e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
  }
}
