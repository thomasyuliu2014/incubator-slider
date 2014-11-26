/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web.view;

import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.slider.server.appmaster.web.SliderAMWebApp;
import org.apache.slider.server.appmaster.web.rest.RestPaths;

/**
 * 
 */
public class NavBlock extends HtmlBlock {

  @Override
  protected void render(Block html) {
    html.
      div("#nav").
        h3("Slider").
        ul().
          li().a(this.prefix(), "Overview")._().
          li().a(this.prefix() + SliderAMWebApp.CONTAINER_STATS, "Statistics")._().
          li().a(this.prefix() + SliderAMWebApp.CLUSTER_SPEC, "Specification")._().
          li().a(rootPath(RestPaths.SYSTEM_METRICS), "Metrics")._().
          li().a(rootPath(RestPaths.SYSTEM_HEALTHCHECK), "Health")._().
          li().a(rootPath(RestPaths.SYSTEM_THREADS), "Threads")._()
        ._()
      ._();
  }

  private String rootPath(String absolutePath) {
    return root_url(absolutePath);
  }
}
