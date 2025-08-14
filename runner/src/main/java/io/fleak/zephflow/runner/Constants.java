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
package io.fleak.zephflow.runner;

/** Created by bolei on 3/5/25 */
public interface Constants {
  String METRIC_NAME_PIPELINE_INPUT_EVENT = "pipeline_input_event_count";
  String METRIC_NAME_PIPELINE_INPUT_SIZE_BYTES = "pipeline_input_size_bytes";
  String METRIC_NAME_PIPELINE_TOTAL_SIZE_BYTES = "pipeline_total_size_bytes";
  String METRIC_NAME_PIPELINE_OUTPUT_EVENT = "pipeline_output_event_count";
  String METRIC_NAME_PIPELINE_ERROR_EVENT = "pipeline_error_event_count";
  String METRIC_NAME_REQUEST_PROCESS_TIME_MILLIS = "request_process_time_millis";
  String METRIC_NAME_NUM_REQUESTS = "num_requests";
  String TAG_FIELD_NAME = "__tag__";

  String HTTP_STARTER_WORKFLOW_CONTROLLER_PATH = "/api/v1/workflows";
  String HTTP_STARTER_EXECUTION_CONTROLLER_PATH = "/api/v1/execution";
}
