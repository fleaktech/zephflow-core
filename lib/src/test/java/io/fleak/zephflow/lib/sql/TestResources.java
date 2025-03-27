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
package io.fleak.zephflow.lib.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class TestResources {

    public static Map<String, Map<String, List<String>>> getTestQueries() {
        try {
            var queryData = Files.readString(Path.of("src", "test","resources", "sql", "queries.yaml"));
            ObjectMapper mapper = new YAMLMapper();
            return mapper.readValue(queryData, Map.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, List<String>> getSqlAsAstQueries() {
        try {
            var queryData = Files.readString(Path.of("src", "test", "resources", "sql", "sql_to_ast.yaml"));
            ObjectMapper mapper = new YAMLMapper();
            return mapper.readValue(queryData, Map.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
