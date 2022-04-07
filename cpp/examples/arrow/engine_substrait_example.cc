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

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/engine/api.h>
#include <arrow/engine/substrait/util.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/iterator.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <vector>

namespace eng = arrow::engine;
namespace cp = arrow::compute;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

std::string GetSubstraitPlanFromServer(const std::string& filename) {
  // Emulate server interaction by parsing hard coded JSON
  std::string substrait_json = R"({
    "relations": [
      {"rel": {
        "read": {
          "base_schema": {
            "struct": {
              "types": [ 
                         {"i64": {}},
                         {"bool": {}}
                       ]
            },
            "names": [
                      "i",
                       "b"
                     ]
          },
          "local_files": {
            "items": [
              {
                "uri_file": "file://FILENAME_PLACEHOLDER",
                "format": "FILE_FORMAT_PARQUET"
              }
            ]
          }
        }
      }}
    ]
  })";
  std::string filename_placeholder = "FILENAME_PLACEHOLDER";
  substrait_json.replace(substrait_json.find(filename_placeholder),
                         filename_placeholder.size(), filename);
  return substrait_json;
}

arrow::Status Main(std::string substrait_json) {
  cp::ExecContext exec_context;

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

  ARROW_ASSIGN_OR_RAISE(auto plan, cp::ExecPlan::Make());

  arrow::engine::SubstraitExecutor executor(substrait_json, &sink_gen, plan,
                                            exec_context);

  ARROW_ASSIGN_OR_RAISE(auto sink_reader, executor.Execute());

  ARROW_ASSIGN_OR_RAISE(auto table,
                        arrow::Table::FromRecordBatchReader(sink_reader.get()));

  std::cout << "Results : " << table->ToString() << std::endl;

  RETURN_NOT_OK(executor.Close());

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cout << "Please specify a parquet file to scan" << std::endl;
    // Fake pass for CI
    return EXIT_SUCCESS;
  }
  auto substrait_json = GetSubstraitPlanFromServer(argv[1]);

  auto status = Main(substrait_json);

  if (!status.ok()) {
    std::cout << "Error ocurred: " << status.message() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
