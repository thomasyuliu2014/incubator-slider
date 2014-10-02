/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.agent.actions

import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.client.SliderClient
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Before
import org.junit.Test

/**
 * Test List operations
 */
@Slf4j

class TestActionDiagnostic extends AgentMiniClusterTestBase {

  @Before
  public void setup() {
    super.setup()
    createMiniCluster("", configuration, 1, false)
  }

  /**
   * This is a test suite to run the tests against a single cluster instance
   * for faster test runs
   * @throws Throwable
   */

  @Test
  public void testSuite() throws Throwable {
    testDiagnosticClient()
    testDiagnosticApplication()
    testDiagnosticSlider()
    testDiagnosticYarn()
    testDiagnosticCredential()
    testDiagnosticAll()
    testDiagnosticIntelligently()
  }
  
  public void testDiagnosticClient() throws Throwable {
    log.info("RM address = ${RMAddr}")
    ServiceLauncher<SliderClient> launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            SliderActions.ACTION_DIAGNOSTIC,
            Arguments.ARG_CLIENT
        ]
    )
    assert launcher.serviceExitCode == 0
  }
  
  public void testDiagnosticApplication() throws Throwable {
    //launch the cluster
    String clustername = createClusterName()
    ServiceLauncher<SliderClient> launcher = createStandaloneAM(
        clustername,
        true,
        false)
    addToTeardown(launcher)
    //do the low level operations to get a better view of what is going on 
    SliderClient sliderClient = launcher.service
    waitForClusterLive(sliderClient)
    
    log.info("what what what" )
    
    //now diagnostic
    launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        [
            SliderActions.ACTION_DIAGNOSTIC,
            Arguments.ARG_APPLICATION,
            clustername
        ]
    )
    assert launcher.serviceExitCode == 0
    //now look for the explicit sevice
  }
  
  public void testDiagnosticSlider() throws Throwable {
    //launch the cluster
    String clustername = createClusterName()
    ServiceLauncher<SliderClient> launcher = createStandaloneAM(
        clustername,
        true,
        false)
    addToTeardown(launcher)
    //do the low level operations to get a better view of what is going on
    SliderClient sliderClient = launcher.service
    waitForClusterLive(sliderClient)

    //now diagnostic
    launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        [
            SliderActions.ACTION_DIAGNOSTIC,
            Arguments.ARG_SLIDER,
            clustername
        ]
    )
    
    assert launcher.serviceExitCode == 0
    //now look for the explicit sevice
  }
  
  public void testDiagnosticYarn() throws Throwable {
    log.info("RM address = ${RMAddr}")
    ServiceLauncher<SliderClient> launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            SliderActions.ACTION_DIAGNOSTIC,
            Arguments.ARG_YARN
        ]
    )
    assert launcher.serviceExitCode == 0
  }
  
  public void testDiagnosticCredential() throws Throwable {
    log.info("RM address = ${RMAddr}")
    ServiceLauncher<SliderClient> launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            SliderActions.ACTION_DIAGNOSTIC,
            Arguments.ARG_CREDENTIALS
        ]
    )
    assert launcher.serviceExitCode == 0
  }
  
  public void testDiagnosticAll() throws Throwable {
    //launch the cluster
    String clustername = createClusterName()
    ServiceLauncher<SliderClient> launcher = createStandaloneAM(
        clustername,
        true,
        false)
    addToTeardown(launcher)
    //do the low level operations to get a better view of what is going on
    SliderClient sliderClient = launcher.service
    waitForClusterLive(sliderClient)

    //now diagnostic
    launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        [
            SliderActions.ACTION_DIAGNOSTIC,
            Arguments.ARG_ALL,
            clustername
        ]
    )
    assert launcher.serviceExitCode == 0
    //now look for the explicit sevice
  }
  
  public void testDiagnosticIntelligently() throws Throwable {
    //launch the cluster
    String clustername = createClusterName()
    ServiceLauncher<SliderClient> launcher = createStandaloneAM(
        clustername,
        true,
        false)
    addToTeardown(launcher)
    //do the low level operations to get a better view of what is going on
    SliderClient sliderClient = launcher.service
    waitForClusterLive(sliderClient)

    //now diagnostic
    launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        [
            SliderActions.ACTION_DIAGNOSTIC,
            Arguments.ARG_LEVEL,
            clustername
        ]
    )
    assert launcher.serviceExitCode == 0
    //now look for the explicit sevice
  }
}
