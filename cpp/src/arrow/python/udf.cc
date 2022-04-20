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

namespace arrow {

namespace py {

struct PythonUdf {
  std::shared_ptr<OwnedRefNoGIL> function;
  compute::OutputType output_type;

  // function needs to be destroyed at process exit
  // and Python may no longer be initialized.
  ~PythonUdf() {
    if (_Py_IsFinalizing()) {
      function->detach();
    }
  }

  Status operator()(compute::KernelContext* ctx, const compute::ExecBatch& batch,
                    Datum* out) {
    return SafeCallIntoPython([=]() -> Status { return Execute(ctx, batch, out); });
  }

  Status Execute(compute::KernelContext* ctx, const compute::ExecBatch& batch,
                 Datum* out) {
    const auto num_args = batch.values.size();
    PyObject* arg_tuple = PyTuple_New(num_args);
    for (size_t arg_id = 0; arg_id < num_args; arg_id++) {
      switch (batch[arg_id].kind()) {
        case Datum::SCALAR: {
          auto c_data = batch[arg_id].scalar();
          PyObject* data = wrap_scalar(c_data);
          PyTuple_SetItem(arg_tuple, arg_id, data);
          break;
        }
        case Datum::ARRAY: {
          auto c_data = batch[arg_id].make_array();
          PyObject* data = wrap_array(c_data);
          PyTuple_SetItem(arg_tuple, arg_id, data);
          break;
        }
        default:
          return Status::NotImplemented(
              "User-defined-functions are not supported for the datum kind ",
              batch[arg_id].kind());
      }
    }
    PyObject* result;
    result = PyObject_CallObject(function->obj(), arg_tuple);
    RETURN_NOT_OK(CheckPyError());
    if (result == Py_None) {
      return Status::Invalid("Output is None, but expected an array");
    }
    // unwrapping the output for expected output type
    if (is_scalar(result)) {
      ARROW_ASSIGN_OR_RAISE(auto val, unwrap_scalar(result));
      if (!output_type.type()->Equals(val->type)) {
        return Status::TypeError("Expected output type, ", output_type.type()->ToString(),
                                 ", but function returned type ", val->type->ToString());
      }
      *out = Datum(val);
      return Status::OK();
    } else if (is_array(result)) {
      ARROW_ASSIGN_OR_RAISE(auto val, unwrap_array(result));
      if (!output_type.type()->Equals(val->type())) {
        return Status::TypeError("Expected output type, ", output_type.type()->ToString(),
                                 ", but function returned type ",
                                 val->type()->ToString());
      }
      *out = Datum(val);
      return Status::OK();
    } else {
      return Status::TypeError("Unexpected output type: ", Py_TYPE(result)->tp_name,
                               " (expected Scalar or Array)");
    }
    return Status::OK();
  }
};

Status RegisterScalarFunction(PyObject* function, const ScalarUdfOptions& options) {
  if (function == nullptr) {
    return Status::Invalid("Python function cannot be null");
  }
  if (!PyCallable_Check(function)) {
    return Status::TypeError("Expected a callable Python object.");
  }
  auto doc = options.doc();
  auto arity = options.arity();
  auto in_types = options.input_types();
  auto exp_out_type = options.output_type();
  auto scalar_func =
      std::make_shared<compute::ScalarFunction>(options.name(), arity, std::move(doc));
  Py_INCREF(function);
  PythonUdf exec{std::make_shared<OwnedRefNoGIL>(function), std::move(exp_out_type)};
  compute::ScalarKernel kernel(
      compute::KernelSignature::Make(options.input_types(), options.output_type(),
                                     arity.is_varargs),
      std::move(exec));
  kernel.mem_allocation = compute::MemAllocation::NO_PREALLOCATE;
  kernel.null_handling = compute::NullHandling::COMPUTED_NO_PREALLOCATE;
  RETURN_NOT_OK(scalar_func->AddKernel(std::move(kernel)));
  auto registry = compute::GetFunctionRegistry();
  RETURN_NOT_OK(registry->AddFunction(std::move(scalar_func)));
  return Status::OK();
}

}  // namespace py

}  // namespace arrow
