// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// NOTE: API is EXPERIMENTAL and will change without going through a
// deprecation cycle

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/engine/substrait/extension_types.h"
#include "arrow/engine/substrait/serde.h"
#include "arrow/engine/substrait/visibility.h"
#include "arrow/type_fwd.h"

#include "substrait/algebra.pb.h"  // IWYU pragma: export

namespace arrrow {

namespace engine {

using SubstraitConverter =
    std::function<std::tuple<substrait::Rel, Schema>(Schema, Declaration)>;

class ARROW_EXPORT SubstraitConversionRegistry {
 public:
  ~SubstraitConversionRegistry();

  static std::unique_ptr<SubstraitConversionRegistry> Make();

  static std::unique_ptr<SubstraitConversionRegistry> Make(
      SubstraitConversionRegistry* parent);

  Status RegisterConverter(const std::string& kind_name, SubstraitConverter converter);

 private:
  SubstraitConversionRegistry();

  class SubstraitConversionRegistryImpl;
  std::unique_ptr<SubstraitConversionRegistryImpl> impl_;

  explicit SubstraitConversionRegistry(SubstraitConversionRegistryImpl* impl);
};

ARROW_EXPORT SubstraitConversionRegistry* GetSubstraitConversionRegistry();

}  // namespace engine
}  // namespace arrrow
