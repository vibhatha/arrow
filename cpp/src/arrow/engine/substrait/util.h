// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <optional>

#include "arrow/compute/registry.h"
#include "arrow/compute/exec/options.h"
#include "arrow/engine/substrait/api.h"
#include "arrow/engine/substrait/options.h"
#include "arrow/util/iterator.h"

namespace arrow {

namespace engine {

using PythonTableProvider =
    std::function<Result<std::shared_ptr<Table>>(const std::vector<std::string>&)>;

/// \brief Get a RecordBatchReader by executing a serialized Substrait plan.
///
/// \param[in] substrait_buffer a buffer containing the protobuf serialization of a
/// Substrait Plan message.
/// \param[in] ext_id_registry an extension-id-registry to use, or
/// null for the default one.
/// \param[in] func_registry a function registry to use, or null
/// for the default one.
/// \param[in] conversion_options options to control how the
/// conversion is to be done.
/// \param[in] backpressure_options options to control how the backpressure is
/// handled
/// \param[out] monitor a pointer to an object which monitors the backpressure
ARROW_ENGINE_EXPORT Result<std::shared_ptr<RecordBatchReader>> ExecuteSerializedPlan(
    const Buffer& substrait_buffer, const ExtensionIdRegistry* ext_id_registry = NULLPTR,
    compute::FunctionRegistry* func_registry = NULLPTR,
    const ConversionOptions& conversion_options = {},
    compute::BackpressureOptions backpressure_options = {},
    compute::BackpressureMonitor** monitor = NULLPTR);

/// \brief Get a Serialized Plan from a Substrait JSON plan.
/// This is a helper method for Python tests.
ARROW_ENGINE_EXPORT Result<std::shared_ptr<Buffer>> SerializeJsonPlan(
    const std::string& substrait_json);

/// \brief Make a nested registry with the default registry as parent.
/// See arrow::engine::nested_extension_id_registry for details.
ARROW_ENGINE_EXPORT std::shared_ptr<ExtensionIdRegistry> MakeExtensionIdRegistry();

ARROW_ENGINE_EXPORT const std::string& default_extension_types_uri();

}  // namespace engine

}  // namespace arrow
