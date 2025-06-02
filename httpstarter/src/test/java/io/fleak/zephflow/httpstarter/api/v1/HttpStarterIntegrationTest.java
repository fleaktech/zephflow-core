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
package io.fleak.zephflow.httpstarter.api.v1;

import static io.fleak.zephflow.lib.utils.JsonUtils.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.httpstarter.dto.ExecuteDto;
import io.fleak.zephflow.httpstarter.dto.WorkflowDto;
import io.fleak.zephflow.lib.utils.YamlUtils;
import io.fleak.zephflow.runner.NoSourceDagRunner;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.context.WebApplicationContext;

/** Created by bolei on 3/6/25 */
@ExtendWith(SpringExtension.class)
@SpringBootTest
@AutoConfigureMockMvc
class HttpStarterIntegrationTest {
  @Autowired private WebApplicationContext webApplicationContext;

  private MockMvc mvc;

  @Autowired
  private ConcurrentHashMap<
          String, Pair<List<AdjacencyListDagDefinition.DagNode>, NoSourceDagRunner>>
      dagMap;

  @BeforeEach
  public void setUp() {
    mvc = webAppContextSetup(webApplicationContext).build();
    dagMap.clear();
  }

  /*
   * create a workflow
   * list all workflows and see the one just created
   * send events to the newly created workflow
   * delete that workflow
   * send events to the same workflow, get 404
   * list all workflows, get empty list
   * */
  @Test
  public void test_createWorkflowAndHitEndpoint() throws Exception {

    // create a workflow
    AdjacencyListDagDefinition dagDefinition =
        YamlUtils.fromYamlResource("/test_dag.yml", new TypeReference<>() {});
    var request = WorkflowDto.Request.builder().dag(dagDefinition.getDag()).build();
    MvcResult result;
    result =
        mvc.perform(
                post("/api/v1/workflows")
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON)
                    .content(Objects.requireNonNull(toJsonString(request))))
            .andExpect(status().isOk())
            .andReturn();
    WorkflowDto.Response createWorkflowResp =
        fromJsonString(result.getResponse().getContentAsString(), new TypeReference<>() {});
    assertEquals(dagDefinition.getDag(), Objects.requireNonNull(createWorkflowResp).getDag());

    // list endpoints
    result =
        mvc.perform(
                get("/api/v1/workflows")
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON)
                    .content(Objects.requireNonNull(toJsonString(request))))
            .andExpect(status().isOk())
            .andReturn();
    List<WorkflowDto.Response> listWorkflowResp =
        fromJsonString(result.getResponse().getContentAsString(), new TypeReference<>() {});
    assertEquals(1, Objects.requireNonNull(listWorkflowResp).size());
    assertEquals(createWorkflowResp, listWorkflowResp.get(0));

    // send event to workflow
    int eventCount = 10;
    List<Map<String, Object>> sourceEvents = new ArrayList<>();
    for (int i = 0; i < eventCount; ++i) {
      sourceEvents.add(Map.of("num", i));
    }

    var workflowId = createWorkflowResp.getId();
    result =
        mvc.perform(
                post(String.format("/api/v1/execution/run/%s/batch", workflowId))
                    .queryParam("includeErrorByStep", "true")
                    .queryParam("includeOutputByStep", "true")
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON)
                    .content(Objects.requireNonNull(toJsonString(sourceEvents))))
            .andExpect(status().isOk())
            .andReturn();
    ExecuteDto.Response actual =
        fromJsonString(result.getResponse().getContentAsString(), new TypeReference<>() {});
    System.out.println(toJsonString(actual));
    ExecuteDto.Response expected = fromJsonResource("/test_output.json", new TypeReference<>() {});
    assertEquals(expected.getOutput(), Objects.requireNonNull(actual).getOutput());

    // delete the workflow
    mvc.perform(delete(String.format("/api/v1/workflows/%s", workflowId)))
        .andExpect(status().isOk())
        .andReturn();

    // delete again, not found
    mvc.perform(delete(String.format("/api/v1/workflows/%s", workflowId)))
        .andExpect(status().isNotFound())
        .andReturn();

    // send events to the workflow, not found
    mvc.perform(
            post(String.format("/api/v1/execution/run/%s/batch", workflowId))
                .queryParam("includeErrorByStep", "true")
                .queryParam("includeOutputByStep", "true")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .content(Objects.requireNonNull(toJsonString(sourceEvents))))
        .andExpect(status().isNotFound())
        .andReturn();

    // list workflows again, should see empty list
    result =
        mvc.perform(
                get("/api/v1/workflows")
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON)
                    .content(Objects.requireNonNull(toJsonString(request))))
            .andExpect(status().isOk())
            .andReturn();
    listWorkflowResp =
        fromJsonString(result.getResponse().getContentAsString(), new TypeReference<>() {});
    assertTrue(Objects.requireNonNull(listWorkflowResp).isEmpty());
  }
}
