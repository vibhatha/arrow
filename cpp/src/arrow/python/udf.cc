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

#include "arrow/python/udf.h"

#include <cstddef>
#include <memory>
#include <sstream>

#include "arrow/compute/function.h"
#include "arrow/python/common.h"

namespace cp = arrow::compute;

namespace arrow {

namespace py {

#define DEFINE_CALL_UDF(TYPE_NAME, FUNCTION_SUFFIX, CONVERT_SUFFIX)                      \
  Status exec_function_##FUNCTION_SUFFIX(const cp::ExecBatch& batch, PyObject* function, \
                                         int num_args, Datum* out) {                     \
    std::shared_ptr<TYPE_NAME> c_res_data;                                               \
    PyObject* result = NULLPTR;                                                          \
    PyObject* data = NULLPTR;                                                            \
    PyObject* arg_tuple = NULLPTR;                                                       \
    Py_XINCREF(data);                                                                    \
    Py_XINCREF(arg_tuple);                                                               \
    Py_XINCREF(result);                                                                  \
    arg_tuple = PyTuple_New(num_args);                                                   \
    for (int arg_id = 0; arg_id < num_args; arg_id++) {                                  \
      if (!batch[arg_id].is_##FUNCTION_SUFFIX()) {                                       \
        return Status::Invalid("Input type and data type doesn't match");                \
      }                                                                                  \
      auto c_data = batch[arg_id].CONVERT_SUFFIX();                                      \
      data = wrap_##FUNCTION_SUFFIX(c_data);                                             \
      PyTuple_SetItem(arg_tuple, arg_id, data);                                          \
    }                                                                                    \
    result = PyObject_CallObject(function, arg_tuple);                                   \
    if (result == NULL) {                                                                \
      return Status::ExecutionError("Error occured in computation");                     \
    }                                                                                    \
    auto res = unwrap_##FUNCTION_SUFFIX(result);                                         \
    if (!res.status().ok()) {                                                            \
      return res.status();                                                               \
    }                                                                                    \
    c_res_data = res.ValueOrDie();                                                       \
    auto datum = new Datum(c_res_data);                                                  \
    *out = *datum;                                                                       \
    Py_XDECREF(data);                                                                    \
    Py_XDECREF(arg_tuple);                                                               \
    Py_XDECREF(result);                                                                  \
    return Status::OK();                                                                 \
  }

DEFINE_CALL_UDF(Scalar, scalar, scalar)
DEFINE_CALL_UDF(Array, array, make_array)

#undef DEFINE_CALL_UDF

Status VerifyArityAndInput(cp::Arity arity, const cp::ExecBatch& batch) {
  bool match = (uint64_t)arity.num_args == batch.values.size();
  if (!match) {
    return Status::Invalid(
        "Function Arity and Input data shape doesn't match, expceted {}");
  }
  return Status::OK();
}

Status ScalarUdfBuilder::MakeFunction(PyObject* function) {
  Status st;
  auto func =
      std::make_shared<cp::ScalarFunction>(this->name(), this->arity(), &this->doc());
  // lambda function
  auto call_back_lambda = [function, this](cp::KernelContext* ctx,
                                           const cp::ExecBatch& batch,
                                           Datum* out) -> Status {
    PyAcquireGIL lock;
    if (function == NULL) {
      return Status::ExecutionError("python function cannot be null");
    }

    if (PyCallable_Check(function)) {
      RETURN_NOT_OK(VerifyArityAndInput(this->arity(), batch));
      if (batch[0].is_array()) {  // checke 0-th element to select array callable
        RETURN_NOT_OK(exec_function_array(batch, function, this->arity().num_args, out));
      } else if (batch[0].is_scalar()) {  // check 0-th element to select scalar callable
        RETURN_NOT_OK(exec_function_scalar(batch, function, this->arity().num_args, out));
      } else {
        return Status::Invalid("Unexpected input type, scalar or array type expected.");
      }
    } else {
      return Status::ExecutionError("Expected a callable python object.");
    }
    return Status::OK();
  };

  // lambda function
  cp::ScalarKernel kernel(this->input_types(), this->output_type(), call_back_lambda);
  kernel.mem_allocation = this->mem_allocation();
  kernel.null_handling = this->null_handling();
  st = func->AddKernel(std::move(kernel));
  if (!st.ok()) {
    return Status::ExecutionError("Kernel couldn't be added to the udf : " +
                                  st.message());
  }
  auto registry = cp::GetFunctionRegistry();
  st = registry->AddFunction(std::move(func));
  if (!st.ok()) {
    return Status::ExecutionError("udf registration failed : " + st.message());
  }
  return Status::OK();
}

}  // namespace py

}  // namespace arrow
