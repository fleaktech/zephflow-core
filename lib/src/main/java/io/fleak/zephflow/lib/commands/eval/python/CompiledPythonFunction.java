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
package io.fleak.zephflow.lib.commands.eval.python;

import lombok.NonNull;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

/** Created by bolei on 4/22/25 */
public record CompiledPythonFunction(
    String discoveredFunctionName, // For logging/debugging
    @NonNull Value functionValue, // The executable GraalVM Value
    @NonNull Context pythonContext) {}
