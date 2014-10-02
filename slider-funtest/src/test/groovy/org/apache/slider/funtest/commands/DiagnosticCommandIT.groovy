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

package org.apache.slider.funtest.commands

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import org.apache.bigtop.itest.shell.Shell
import org.apache.slider.funtest.framework.CommandTestBase
import org.apache.slider.funtest.framework.SliderShell
import org.apache.slider.common.params.SliderActions
import org.junit.Test
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.hadoop.fs.Path
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderXMLConfKeysForTesting
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.tools.SliderUtils
import org.apache.tools.zip.ZipEntry
import org.apache.tools.zip.ZipOutputStream
import org.junit.After;
import org.junit.Before
import org.junit.Rule
import org.junit.rules.TemporaryFolder


@Slf4j
public class DiagnosticCommandIT extends CommandTestBase implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {

    public static final boolean AGENTTESTS_ENABLED
    public static final boolean AGENTTESTS_QUEUE_LABELED_DEFINED
    public static final boolean AGENTTESTS_LABELS_RED_BLUE_DEFINED
    private static String TEST_APP_PKG_DIR_PROP = "test.app.pkg.dir"
    private static String TEST_APP_PKG_FILE_PROP = "test.app.pkg.file"
    private static String TEST_APP_PKG_NAME_PROP = "test.app.pkg.name"
    private static String TEST_APP_RESOURCE = "test.app.resource"
    private static String TEST_APP_TEMPLATE = "test.app.template"

    protected String APP_RESOURCE = getAppResource()
    protected String APP_TEMPLATE = getAppTemplate()
    public static final String TEST_APP_PKG_DIR = sysprop(TEST_APP_PKG_DIR_PROP)
    public static final String TEST_APP_PKG_FILE = sysprop(TEST_APP_PKG_FILE_PROP)
    public static final String TEST_APP_PKG_NAME = sysprop(TEST_APP_PKG_NAME_PROP)

    private static String APPLICATION_NAME = "happy-path-with-flex"

    @Before
    public void setupApplicationPackage() {
        ensureAppCleaned()
        
        try {
            File zipFileName = new File(TEST_APP_PKG_DIR, TEST_APP_PKG_FILE).canonicalFile
            SliderShell shell = slider(EXIT_SUCCESS,
                    [
                        ACTION_INSTALL_PACKAGE,
                        Arguments.ARG_NAME,
                        TEST_APP_PKG_NAME,
                        Arguments.ARG_PACKAGE,
                        zipFileName,
                        Arguments.ARG_REPLACE_PKG
                    ])
            logShell(shell)
            log.info "App pkg uploaded at home directory .slider/package/$TEST_APP_PKG_NAME/$TEST_APP_PKG_FILE"
        } catch (Exception e) {
            throw e;
        }
        
        setUpApp()
    }
    
    public void setUpApp() throws Throwable {
        SliderShell shell = slider(EXIT_SUCCESS,
                [
                    SliderActions.ACTION_CREATE,
                    APPLICATION_NAME,
                    ARG_TEMPLATE,
                    APP_TEMPLATE,
                    ARG_RESOURCES,
                    APP_RESOURCE
                ])
        logShell(shell)
        ensureApplicationIsUp(APPLICATION_NAME)
    }


    public void ensureAppCleaned(){
        cleanup(APPLICATION_NAME)
    }

    @Test
    public void testDiagnosticClient() throws Throwable {
        SliderShell shell = slider(0, [SliderActions.ACTION_DIAGNOSTIC, Arguments.ARG_CLIENT])
        logShell(shell)
        assertSuccess(shell)
    }
    
    @Test
    public void testDiagnosticSlider() throws Throwable {
        SliderShell shell = slider(0, [SliderActions.ACTION_DIAGNOSTIC, Arguments.ARG_SLIDER, APPLICATION_NAME])
        logShell(shell)
        assertSuccess(shell)
    }
    
    @Test
    public void testDiagnosticApp() throws Throwable {
        SliderShell shell = slider(0, [SliderActions.ACTION_DIAGNOSTIC, Arguments.ARG_APPLICATION, APPLICATION_NAME])
        logShell(shell)
        assertSuccess(shell)
    }
    
    @Test
    public void testDiagnosticYarn() throws Throwable {
        SliderShell shell = slider(0, [SliderActions.ACTION_DIAGNOSTIC, Arguments.ARG_YARN])
        logShell(shell)
        assertSuccess(shell)
    }
    
    @Test
    public void testDiagnosticCred() throws Throwable {
        SliderShell shell = slider(0, [SliderActions.ACTION_DIAGNOSTIC, Arguments.ARG_CREDENTIALS])
        logShell(shell)
        assertSuccess(shell)
    }
    
    @Test
    public void testDiagnosticAll() throws Throwable {
        SliderShell shell = slider(0, [SliderActions.ACTION_DIAGNOSTIC, Arguments.ARG_ALL, APPLICATION_NAME])
        logShell(shell)
        assertSuccess(shell)
    }
    
    @Test
    public void testDiagnosticLevel() throws Throwable {
        SliderShell shell = slider(0, [SliderActions.ACTION_DIAGNOSTIC, Arguments.ARG_LEVEL, APPLICATION_NAME])
        logShell(shell)
        assertSuccess(shell)
    }

    @After
    public void destroyCluster() {
        cleanup(APPLICATION_NAME)
    }

    protected void cleanup(String applicationName) throws Throwable {

        log.info "Cleaning app instance, if exists, by name " + applicationName
        teardown(applicationName)

        // sleep till the instance is frozen
        sleep(1000 * 3)

        SliderShell shell = slider([
            ACTION_DESTROY,
            applicationName
        ])

        if (shell.ret != 0 && shell.ret != EXIT_UNKNOWN_INSTANCE) {
            logShell(shell)
            assert fail("Old cluster either should not exist or should get destroyed.")
        }
    }

    protected String getAppResource() {
        return sysprop(TEST_APP_RESOURCE)
    }

    protected String getAppTemplate() {
        return sysprop(TEST_APP_TEMPLATE)
    }

    public static void logShell(SliderShell shell) {
        for (String str in shell.out) {
            log.info str
        }
        for (String str in shell.err) {
            log.error str
        }
    }
}
