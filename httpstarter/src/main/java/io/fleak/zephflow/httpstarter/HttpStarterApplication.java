/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.httpstarter;

import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.serdes.des.csv.CsvDeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.jsonarr.JsonArrayDeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.strline.StringLineDeserializerFactory;
import io.fleak.zephflow.runner.DagCompiler;
import io.fleak.zephflow.runner.NoSourceDagRunner;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/** Created by bolei on 3/4/25 */
@SpringBootApplication
public class HttpStarterApplication {
  public static void main(String[] args) {
    SpringApplication.run(HttpStarterApplication.class, args);
  }

  @Bean
  public ConcurrentHashMap<
          String, Pair<List<AdjacencyListDagDefinition.DagNode>, NoSourceDagRunner>>
      dagMap() {
    return new ConcurrentHashMap<>();
  }

  @Bean
  public DagCompiler dagCompiler() {
    return new DagCompiler(OperatorCommandRegistry.OPERATOR_COMMANDS);
  }

  @Bean
  public CsvDeserializerFactory csvDeserializerFactory() {
    return new CsvDeserializerFactory();
  }

  @Bean
  public JsonArrayDeserializerFactory jsonArrayDeserializerFactory() {
    return new JsonArrayDeserializerFactory();
  }

  @Bean
  public StringLineDeserializerFactory stringLineDeserializerFactory() {
    return new StringLineDeserializerFactory();
  }
}
