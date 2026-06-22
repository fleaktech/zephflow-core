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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import java.util.List;

/**
 * An SQS message carrying one or more S3 object references. Acts as the commit unit: its {@code
 * receiptHandle} is used to delete the message from SQS once all its referenced objects have been
 * processed.
 */
public record S3EventMessage(
    String messageId, String receiptHandle, List<S3ObjectRef> objectRefs) {}
