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

package org.apache.slider.client

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.slider.common.params.ActionDiagnosticArgs
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.common.params.ClientArgs
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.exceptions.ErrorStrings
import org.apache.slider.core.exceptions.UsageException
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.main.ServiceLauncherBaseTest
import org.junit.Test

/**
 * Test the argument parsing/validation logic
 */
@Slf4j
class TestDiagnosticOption extends ServiceLauncherBaseTest {

    @Test
    public void testDiagnosticUsage() throws Throwable {
        def exception = launchExpectingException(SliderClient,
                new Configuration(),
                ActionDiagnosticArgs.USAGE,
                [
                    SliderActions.ACTION_DIAGNOSTIC
                ])
        assert exception instanceof UsageException
        log.info(exception.toString())
    }

    @Test
    public void testUnknownOption() throws Throwable {
        def exception = launchExpectingException(SliderClient,
                new Configuration(),
                ActionDiagnosticArgs.USAGE,
                [SliderActions.ACTION_DIAGNOSTIC, "not-a-known-option"])
        assert exception instanceof UsageException
        log.info(exception.toString())
    }

    @Test
    public void testSliderOptionWithoutEnoughArgs() throws Throwable {
        def exception = launchExpectingException(SliderClient,
            new Configuration(),
            "Expected a value after parameter " + Arguments.ARG_SLIDER,
            [SliderActions.ACTION_DIAGNOSTIC, Arguments.ARG_SLIDER])
        assert exception instanceof BadCommandArgumentsException
        log.info(exception.toString())
    }
    
    @Test
    public void testApplicationOptionWithoutEnoughArgs() throws Throwable {
        def exception = launchExpectingException(SliderClient,
            new Configuration(),
            "Expected a value after parameter " + Arguments.ARG_APPLICATION,
            [SliderActions.ACTION_DIAGNOSTIC, Arguments.ARG_APPLICATION])
        assert exception instanceof BadCommandArgumentsException
        log.info(exception.toString())
    }
    
    @Test
    public void testAllOptionWithoutEnoughArgs() throws Throwable {
        def exception = launchExpectingException(SliderClient,
            new Configuration(),
            "Expected a value after parameter " + Arguments.ARG_ALL,
            [SliderActions.ACTION_DIAGNOSTIC, Arguments.ARG_ALL])
        assert exception instanceof BadCommandArgumentsException
        log.info(exception.toString())
    }
    
    @Test
    public void testIntelligentOptionWithoutEnoughArgs() throws Throwable {
        def exception = launchExpectingException(SliderClient,
            new Configuration(),
            "Expected a value after parameter " + Arguments.ARG_LEVEL,
            [SliderActions.ACTION_DIAGNOSTIC, Arguments.ARG_LEVEL])
        assert exception instanceof BadCommandArgumentsException
        log.info(exception.toString())
    }

}
