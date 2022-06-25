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

#include "arrow/engine/substrait/plan_internal.h"
#include "arrow/engine/substrait/extension_set.h"
#include "arrow/engine/substrait/serde.h"
#include "arrow/engine/substrait/util.h"

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"

namespace arrow {

namespace engine {

TEST(PlanInternalTest, ExtensionSetSerializationBasic) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", R"({
    "extension_uris": [
      {
        "extension_uri_anchor": 7,
        "uri": "https://github.com/apache/arrow/blob/master/format/substrait/extension_types.yaml"
      }
    ],
    "extensions": [
      {"extension_type": {
        "extension_uri_reference": 7,
        "type_anchor": 42,
        "name": "null"
      }},
      {"extension_function": {
        "extension_uri_reference": 7,
        "function_anchor": 42,
        "name": "add"
      }}
    ]
  })"));
  ExtensionSet ext_set;
  ASSERT_OK_AND_ASSIGN(
      auto sink_decls,
      DeserializePlans(
          *buf, [] { return std::shared_ptr<compute::SinkNodeConsumer>{nullptr}; },
          &ext_set));

  ASSERT_OK_AND_ASSIGN(auto ext_uris, GetExtensionURIs(ext_set));

  EXPECT_EQ(ext_uris[0]->uri(), std::string("https://github.com/apache/arrow/blob/master/"
                                            "format/substrait/extension_types.yaml"));
  EXPECT_EQ(ext_uris[0]->extension_uri_anchor(), 7);
}

}  // namespace engine
}  // namespace arrow
