# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# cython: language_level = 3

import sys

from cpython.object cimport Py_LT, Py_EQ, Py_GT, Py_LE, Py_NE, Py_GE
from cython.operator cimport dereference as deref

from collections import namedtuple

from pyarrow.lib import frombytes, tobytes, ordered_dict
from pyarrow.lib cimport *
from pyarrow.includes.libarrow cimport *
import pyarrow.lib as lib

from cpython.ref cimport PyObject

import numpy as np


cdef wrap_scalar_function(const shared_ptr[CFunction]& sp_func):
    """
    Wrap a C++ scalar Function in a ScalarFunction object.
    """
    cdef ScalarFunction func = ScalarFunction.__new__(ScalarFunction)
    func.init(sp_func)
    return func


cdef wrap_vector_function(const shared_ptr[CFunction]& sp_func):
    """
    Wrap a C++ vector Function in a VectorFunction object.
    """
    cdef VectorFunction func = VectorFunction.__new__(VectorFunction)
    func.init(sp_func)
    return func


cdef wrap_scalar_aggregate_function(const shared_ptr[CFunction]& sp_func):
    """
    Wrap a C++ aggregate Function in a ScalarAggregateFunction object.
    """
    cdef ScalarAggregateFunction func = \
        ScalarAggregateFunction.__new__(ScalarAggregateFunction)
    func.init(sp_func)
    return func


cdef wrap_hash_aggregate_function(const shared_ptr[CFunction]& sp_func):
    """
    Wrap a C++ aggregate Function in a HashAggregateFunction object.
    """
    cdef HashAggregateFunction func = \
        HashAggregateFunction.__new__(HashAggregateFunction)
    func.init(sp_func)
    return func


cdef wrap_meta_function(const shared_ptr[CFunction]& sp_func):
    """
    Wrap a C++ meta Function in a MetaFunction object.
    """
    cdef MetaFunction func = MetaFunction.__new__(MetaFunction)
    func.init(sp_func)
    return func


cdef wrap_function(const shared_ptr[CFunction]& sp_func):
    """
    Wrap a C++ Function in a Function object.

    This dispatches to specialized wrappers depending on the function kind.
    """
    if sp_func.get() == NULL:
        raise ValueError("Function was NULL")

    cdef FunctionKind c_kind = sp_func.get().kind()
    if c_kind == FunctionKind_SCALAR:
        return wrap_scalar_function(sp_func)
    elif c_kind == FunctionKind_VECTOR:
        return wrap_vector_function(sp_func)
    elif c_kind == FunctionKind_SCALAR_AGGREGATE:
        return wrap_scalar_aggregate_function(sp_func)
    elif c_kind == FunctionKind_HASH_AGGREGATE:
        return wrap_hash_aggregate_function(sp_func)
    elif c_kind == FunctionKind_META:
        return wrap_meta_function(sp_func)
    else:
        raise NotImplementedError("Unknown Function::Kind")


cdef wrap_scalar_kernel(const CScalarKernel* c_kernel):
    if c_kernel == NULL:
        raise ValueError("Kernel was NULL")
    cdef ScalarKernel kernel = ScalarKernel.__new__(ScalarKernel)
    kernel.init(c_kernel)
    return kernel


cdef wrap_vector_kernel(const CVectorKernel* c_kernel):
    if c_kernel == NULL:
        raise ValueError("Kernel was NULL")
    cdef VectorKernel kernel = VectorKernel.__new__(VectorKernel)
    kernel.init(c_kernel)
    return kernel


cdef wrap_scalar_aggregate_kernel(const CScalarAggregateKernel* c_kernel):
    if c_kernel == NULL:
        raise ValueError("Kernel was NULL")
    cdef ScalarAggregateKernel kernel = \
        ScalarAggregateKernel.__new__(ScalarAggregateKernel)
    kernel.init(c_kernel)
    return kernel


cdef wrap_hash_aggregate_kernel(const CHashAggregateKernel* c_kernel):
    if c_kernel == NULL:
        raise ValueError("Kernel was NULL")
    cdef HashAggregateKernel kernel = \
        HashAggregateKernel.__new__(HashAggregateKernel)
    kernel.init(c_kernel)
    return kernel


cdef class Kernel(_Weakrefable):
    """
    A kernel object.

    Kernels handle the execution of a Function for a certain signature.
    """

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly"
                        .format(self.__class__.__name__))


cdef class ScalarKernel(Kernel):
    cdef const CScalarKernel* kernel

    cdef void init(self, const CScalarKernel* kernel) except *:
        self.kernel = kernel

    def __repr__(self):
        return ("ScalarKernel<{}>"
                .format(frombytes(self.kernel.signature.get().ToString())))


cdef class VectorKernel(Kernel):
    cdef const CVectorKernel* kernel

    cdef void init(self, const CVectorKernel* kernel) except *:
        self.kernel = kernel

    def __repr__(self):
        return ("VectorKernel<{}>"
                .format(frombytes(self.kernel.signature.get().ToString())))


cdef class ScalarAggregateKernel(Kernel):
    cdef const CScalarAggregateKernel* kernel

    cdef void init(self, const CScalarAggregateKernel* kernel) except *:
        self.kernel = kernel

    def __repr__(self):
        return ("ScalarAggregateKernel<{}>"
                .format(frombytes(self.kernel.signature.get().ToString())))


cdef class HashAggregateKernel(Kernel):
    cdef const CHashAggregateKernel* kernel

    cdef void init(self, const CHashAggregateKernel* kernel) except *:
        self.kernel = kernel

    def __repr__(self):
        return ("HashAggregateKernel<{}>"
                .format(frombytes(self.kernel.signature.get().ToString())))


FunctionDoc = namedtuple(
    "FunctionDoc",
    ("summary", "description", "arg_names", "options_class",
     "options_required"))

cdef wrap_arity(const CArity c_arity):
    cdef Arity arity = Arity.__new__(Arity)
    arity.init(c_arity)
    return arity

cdef wrap_input_type(const CInputType c_input_type):
    cdef InputType input_type = InputType.__new__(InputType)
    input_type.init(c_input_type)
    return input_type

# cdef class FunctionDoc(_Weakrefable):

#     def __init__(self):
#         raise TypeError("Cannot use constructor to initialize FunctionDoc")

#     cdef void init(self, const CFunctionDoc &function_doc):
#         self.function_doc = function_doc

#     @staticmethod
#     def create(self):
#         pass

cdef class InputType(_Weakrefable):

    def __init__(self):
        raise TypeError("Cannot use constructor to initialize InputType")

    cdef void init(self, const CInputType &input_type):
        self.input_type = input_type

    @staticmethod
    def scalar(data_type):
        cdef:
            shared_ptr[CDataType] c_data_type
            CInputType c_input_type
        c_data_type = pyarrow_unwrap_data_type(data_type)
        c_input_type = CInputType.Scalar(c_data_type)
        return wrap_input_type(c_input_type)

    @staticmethod
    def array(data_type):
        cdef:
            shared_ptr[CDataType] c_data_type
            CInputType c_input_type
        c_data_type = pyarrow_unwrap_data_type(data_type)
        c_input_type = CInputType.Array(c_data_type)
        return wrap_input_type(c_input_type)


cdef class Arity(_Weakrefable):

    def __init__(self):
        raise TypeError("Cannot use constructor to initialize Arity")

    cdef void init(self, const CArity &arity):
        self.arity = arity

    @staticmethod
    def unary():
        cdef CArity c_arity = CArity.Unary()
        return wrap_arity(c_arity)

cdef class Function(_Weakrefable):
    """
    A compute function.

    A function implements a certain logical computation over a range of
    possible input signatures.  Each signature accepts a range of input
    types and is implemented by a given Kernel.

    Functions can be of different kinds:

    * "scalar" functions apply an item-wise computation over all items
      of their inputs.  Each item in the output only depends on the values
      of the inputs at the same position.  Examples: addition, comparisons,
      string predicates...

    * "vector" functions apply a collection-wise computation, such that
      each item in the output may depend on the values of several items
      in each input.  Examples: dictionary encoding, sorting, extracting
      unique values...

    * "scalar_aggregate" functions reduce the dimensionality of the inputs by
      applying a reduction function.  Examples: sum, min_max, mode...

    * "hash_aggregate" functions apply a reduction function to an input
      subdivided by grouping criteria.  They may not be directly called.
      Examples: hash_sum, hash_min_max...

    * "meta" functions dispatch to other functions.
    """

    cdef:
        shared_ptr[CFunction] sp_func
        CFunction* base_func

    _kind_map = {
        FunctionKind_SCALAR: "scalar",
        FunctionKind_VECTOR: "vector",
        FunctionKind_SCALAR_AGGREGATE: "scalar_aggregate",
        FunctionKind_HASH_AGGREGATE: "hash_aggregate",
        FunctionKind_META: "meta",
    }

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly"
                        .format(self.__class__.__name__))

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        self.sp_func = sp_func
        self.base_func = sp_func.get()

    def __repr__(self):
        return ("arrow.compute.Function<name={}, kind={}, "
                "arity={}, num_kernels={}>"
                .format(self.name, self.kind, self.arity, self.num_kernels))

    def __reduce__(self):
        # Reduction uses the global registry
        return get_function, (self.name,)

    @property
    def name(self):
        """
        The function name.
        """
        return frombytes(self.base_func.name())

    @property
    def arity(self):
        """
        The function arity.

        If Ellipsis (i.e. `...`) is returned, the function takes a variable
        number of arguments.
        """
        cdef CArity arity = self.base_func.arity()
        if arity.is_varargs:
            return ...
        else:
            return arity.num_args

    @property
    def kind(self):
        """
        The function kind.
        """
        cdef FunctionKind c_kind = self.base_func.kind()
        try:
            return self._kind_map[c_kind]
        except KeyError:
            raise NotImplementedError("Unknown Function::Kind")

    @property
    def _doc(self):
        """
        The C++-like function documentation (for internal use).
        """
        cdef CFunctionDoc c_doc = self.base_func.doc()
        return FunctionDoc(frombytes(c_doc.summary),
                           frombytes(c_doc.description),
                           [frombytes(s) for s in c_doc.arg_names],
                           frombytes(c_doc.options_class),
                           c_doc.options_required)

    @property
    def num_kernels(self):
        """
        The number of kernels implementing this function.
        """
        return self.base_func.num_kernels()

    def call(self, args, FunctionOptions options=None,
             MemoryPool memory_pool=None):
        """
        Call the function on the given arguments.
        """
        cdef:
            const CFunctionOptions* c_options = NULL
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
            CExecContext c_exec_ctx = CExecContext(pool)
            vector[CDatum] c_args
            CDatum result

        _pack_compute_args(args, &c_args)

        if options is not None:
            c_options = options.get_options()

        with nogil:
            result = GetResultValue(
                self.base_func.Execute(c_args, c_options, &c_exec_ctx)
            )

        return wrap_datum(result)


cdef class ScalarFunction(Function):
    cdef const CScalarFunction* func

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        Function.init(self, sp_func)
        self.func = <const CScalarFunction*> sp_func.get()

    @property
    def kernels(self):
        """
        The kernels implementing this function.
        """
        cdef vector[const CScalarKernel*] kernels = self.func.kernels()
        return [wrap_scalar_kernel(k) for k in kernels]


cdef class VectorFunction(Function):
    cdef const CVectorFunction* func

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        Function.init(self, sp_func)
        self.func = <const CVectorFunction*> sp_func.get()

    @property
    def kernels(self):
        """
        The kernels implementing this function.
        """
        cdef vector[const CVectorKernel*] kernels = self.func.kernels()
        return [wrap_vector_kernel(k) for k in kernels]


cdef class ScalarAggregateFunction(Function):
    cdef const CScalarAggregateFunction* func

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        Function.init(self, sp_func)
        self.func = <const CScalarAggregateFunction*> sp_func.get()

    @property
    def kernels(self):
        """
        The kernels implementing this function.
        """
        cdef vector[const CScalarAggregateKernel*] kernels = \
            self.func.kernels()
        return [wrap_scalar_aggregate_kernel(k) for k in kernels]


cdef class HashAggregateFunction(Function):
    cdef const CHashAggregateFunction* func

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        Function.init(self, sp_func)
        self.func = <const CHashAggregateFunction*> sp_func.get()

    @property
    def kernels(self):
        """
        The kernels implementing this function.
        """
        cdef vector[const CHashAggregateKernel*] kernels = self.func.kernels()
        return [wrap_hash_aggregate_kernel(k) for k in kernels]


cdef class MetaFunction(Function):
    cdef const CMetaFunction* func

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        Function.init(self, sp_func)
        self.func = <const CMetaFunction*> sp_func.get()

    # Since num_kernels is exposed, also expose a kernels property
    @property
    def kernels(self):
        """
        The kernels implementing this function.
        """
        return []


cdef _pack_compute_args(object values, vector[CDatum]* out):
    for val in values:
        if isinstance(val, (list, np.ndarray)):
            val = lib.asarray(val)

        if isinstance(val, Array):
            out.push_back(CDatum((<Array> val).sp_array))
            continue
        elif isinstance(val, ChunkedArray):
            out.push_back(CDatum((<ChunkedArray> val).sp_chunked_array))
            continue
        elif isinstance(val, Scalar):
            out.push_back(CDatum((<Scalar> val).unwrap()))
            continue
        elif isinstance(val, RecordBatch):
            out.push_back(CDatum((<RecordBatch> val).sp_batch))
            continue
        elif isinstance(val, Table):
            out.push_back(CDatum((<Table> val).sp_table))
            continue
        else:
            # Is it a Python scalar?
            try:
                scal = lib.scalar(val)
            except Exception:
                # Raise dedicated error below
                pass
            else:
                out.push_back(CDatum((<Scalar> scal).unwrap()))
                continue

        raise TypeError(f"Got unexpected argument type {type(val)} "
                        "for compute function")


cdef class FunctionRegistry(_Weakrefable):
    cdef CFunctionRegistry* registry

    def __init__(self):
        self.registry = GetFunctionRegistry()

    def list_functions(self):
        """
        Return all function names in the registry.
        """
        cdef vector[c_string] names = self.registry.GetFunctionNames()
        return [frombytes(name) for name in names]

    def get_function(self, name):
        """
        Look up a function by name in the registry.

        Parameters
        ----------
        name : str
            The name of the function to lookup
        """
        cdef:
            c_string c_name = tobytes(name)
            shared_ptr[CFunction] func
        with nogil:
            func = GetResultValue(self.registry.GetFunction(c_name))
        return wrap_function(func)

    def register_function(self, name, arity, input_types, output_type, function_kind):
        pass


cdef FunctionRegistry _global_func_registry = FunctionRegistry()


def function_registry():
    return _global_func_registry


def get_function(name):
    """
    Get a function by name.

    The function is looked up in the global registry
    (as returned by `function_registry()`).

    Parameters
    ----------
    name : str
        The name of the function to lookup
    """
    return _global_func_registry.get_function(name)


def list_functions():
    """
    Return all function names in the global registry.
    """
    return _global_func_registry.list_functions()


def call_function(name, args, options=None, memory_pool=None):
    """
    Call a named function.

    The function is looked up in the global registry
    (as returned by `function_registry()`).

    Parameters
    ----------
    name : str
        The name of the function to call.
    args : list
        The arguments to the function.
    options : optional
        options provided to the function.
    memory_pool : MemoryPool, optional
        memory pool to use for allocations during function execution.
    """
    func = _global_func_registry.get_function(name)
    return func.call(args, options=options, memory_pool=memory_pool)


cdef class FunctionOptions(_Weakrefable):
    __slots__ = ()  # avoid mistakingly creating attributes

    cdef const CFunctionOptions* get_options(self) except NULL:
        return self.wrapped.get()

    cdef void init(self, const shared_ptr[CFunctionOptions]& sp):
        self.wrapped = sp

    cdef inline shared_ptr[CFunctionOptions] unwrap(self):
        return self.wrapped

    def serialize(self):
        cdef:
            CResult[shared_ptr[CBuffer]] res = self.get_options().Serialize()
            shared_ptr[CBuffer] c_buf = GetResultValue(res)
        return pyarrow_wrap_buffer(c_buf)

    @staticmethod
    def deserialize(buf):
        """
        Deserialize options for a function.

        Parameters
        ----------
        buf : Buffer
            The buffer containing the data to deserialize.
        """
        cdef:
            shared_ptr[CBuffer] c_buf = pyarrow_unwrap_buffer(buf)
            CResult[unique_ptr[CFunctionOptions]] maybe_options = \
                DeserializeFunctionOptions(deref(c_buf))
            shared_ptr[CFunctionOptions] c_options
        c_options = to_shared(GetResultValue(move(maybe_options)))
        type_name = frombytes(c_options.get().options_type().type_name())
        module = globals()
        if type_name not in module:
            raise ValueError(f'Cannot deserialize "{type_name}"')
        klass = module[type_name]
        options = klass.__new__(klass)
        (<FunctionOptions> options).init(c_options)
        return options

    def __repr__(self):
        type_name = self.__class__.__name__
        # Remove {} so we can use our own braces
        string_repr = frombytes(self.get_options().ToString())[1:-1]
        return f"{type_name}({string_repr})"

    def __eq__(self, FunctionOptions other):
        return self.get_options().Equals(deref(other.get_options()))


def _raise_invalid_function_option(value, description, *,
                                   exception_class=ValueError):
    raise exception_class(f"\"{value}\" is not a valid {description}")


# NOTE:
# To properly expose the constructor signature of FunctionOptions
# subclasses, we use a two-level inheritance:
# 1. a C extension class that implements option validation and setting
#    (won't expose function signatures because of
#     https://github.com/cython/cython/issues/3873)
# 2. a Python derived class that implements the constructor

cdef class _CastOptions(FunctionOptions):
    cdef CCastOptions* options

    cdef void init(self, const shared_ptr[CFunctionOptions]& sp):
        FunctionOptions.init(self, sp)
        self.options = <CCastOptions*> self.wrapped.get()

    def _set_options(self, DataType target_type, allow_int_overflow,
                     allow_time_truncate, allow_time_overflow,
                     allow_decimal_truncate, allow_float_truncate,
                     allow_invalid_utf8):
        cdef:
            shared_ptr[CCastOptions] wrapped = make_shared[CCastOptions]()
        self.init(<shared_ptr[CFunctionOptions]> wrapped)
        self._set_type(target_type)
        if allow_int_overflow is not None:
            self.allow_int_overflow = allow_int_overflow
        if allow_time_truncate is not None:
            self.allow_time_truncate = allow_time_truncate
        if allow_time_overflow is not None:
            self.allow_time_overflow = allow_time_overflow
        if allow_decimal_truncate is not None:
            self.allow_decimal_truncate = allow_decimal_truncate
        if allow_float_truncate is not None:
            self.allow_float_truncate = allow_float_truncate
        if allow_invalid_utf8 is not None:
            self.allow_invalid_utf8 = allow_invalid_utf8

    def _set_type(self, target_type=None):
        if target_type is not None:
            deref(self.options).to_type = \
                (<DataType> ensure_type(target_type)).sp_type

    def _set_safe(self):
        self.init(shared_ptr[CFunctionOptions](
            new CCastOptions(CCastOptions.Safe())))

    def _set_unsafe(self):
        self.init(shared_ptr[CFunctionOptions](
            new CCastOptions(CCastOptions.Unsafe())))

    def is_safe(self):
        return not (deref(self.options).allow_int_overflow or
                    deref(self.options).allow_time_truncate or
                    deref(self.options).allow_time_overflow or
                    deref(self.options).allow_decimal_truncate or
                    deref(self.options).allow_float_truncate or
                    deref(self.options).allow_invalid_utf8)

    @property
    def allow_int_overflow(self):
        return deref(self.options).allow_int_overflow

    @allow_int_overflow.setter
    def allow_int_overflow(self, c_bool flag):
        deref(self.options).allow_int_overflow = flag

    @property
    def allow_time_truncate(self):
        return deref(self.options).allow_time_truncate

    @allow_time_truncate.setter
    def allow_time_truncate(self, c_bool flag):
        deref(self.options).allow_time_truncate = flag

    @property
    def allow_time_overflow(self):
        return deref(self.options).allow_time_overflow

    @allow_time_overflow.setter
    def allow_time_overflow(self, c_bool flag):
        deref(self.options).allow_time_overflow = flag

    @property
    def allow_decimal_truncate(self):
        return deref(self.options).allow_decimal_truncate

    @allow_decimal_truncate.setter
    def allow_decimal_truncate(self, c_bool flag):
        deref(self.options).allow_decimal_truncate = flag

    @property
    def allow_float_truncate(self):
        return deref(self.options).allow_float_truncate

    @allow_float_truncate.setter
    def allow_float_truncate(self, c_bool flag):
        deref(self.options).allow_float_truncate = flag

    @property
    def allow_invalid_utf8(self):
        return deref(self.options).allow_invalid_utf8

    @allow_invalid_utf8.setter
    def allow_invalid_utf8(self, c_bool flag):
        deref(self.options).allow_invalid_utf8 = flag


class CastOptions(_CastOptions):
    """
    Options for the `cast` function.

    Parameters
    ----------
    target_type : DataType, optional
        The PyArrow type to cast to.
    allow_int_overflow : bool, default False
        Whether integer overflow is allowed when casting.
    allow_time_truncate : bool, default False
        Whether time precision truncation is allowed when casting.
    allow_time_overflow : bool, default False
        Whether date/time range overflow is allowed when casting.
    allow_decimal_truncate : bool, default False
        Whether decimal precision truncation is allowed when casting.
    allow_float_truncate : bool, default False
        Whether floating-point precision truncation is allowed when casting.
    allow_invalid_utf8 : bool, default False
        Whether producing invalid utf8 data is allowed when casting.
    """

    def __init__(self, target_type=None, *, allow_int_overflow=None,
                 allow_time_truncate=None, allow_time_overflow=None,
                 allow_decimal_truncate=None, allow_float_truncate=None,
                 allow_invalid_utf8=None):
        self._set_options(target_type, allow_int_overflow, allow_time_truncate,
                          allow_time_overflow, allow_decimal_truncate,
                          allow_float_truncate, allow_invalid_utf8)

    @staticmethod
    def safe(target_type=None):
        """"
        Create a CastOptions for a safe cast.

        Parameters
        ----------
        target_type : optional
            Target cast type for the safe cast.
        """
        self = CastOptions()
        self._set_safe()
        self._set_type(target_type)
        return self

    @staticmethod
    def unsafe(target_type=None):
        """"
        Create a CastOptions for an unsafe cast.

        Parameters
        ----------
        target_type : optional
            Target cast type for the unsafe cast.
        """
        self = CastOptions()
        self._set_unsafe()
        self._set_type(target_type)
        return self


def _skip_nulls_doc():
    # (note the weird indent because of how the string is inserted
    #  by callers)
    return """skip_nulls : bool, default True
        Whether to skip (ignore) nulls in the input.
        If False, any null in the input forces the output to null.
"""


def _min_count_doc(*, default):
    return f"""min_count : int, default {default}
        Minimum number of non-null values in the input.  If the number
        of non-null values is below `min_count`, the output is null.
"""


cdef class _ElementWiseAggregateOptions(FunctionOptions):
    def _set_options(self, skip_nulls):
        self.wrapped.reset(new CElementWiseAggregateOptions(skip_nulls))


class ElementWiseAggregateOptions(_ElementWiseAggregateOptions):
    __doc__ = f"""
    Options for element-wise aggregate functions.

    Parameters
    ----------
    {_skip_nulls_doc()}
    """

    def __init__(self, *, skip_nulls=True):
        self._set_options(skip_nulls)


cdef CRoundMode unwrap_round_mode(round_mode) except *:
    if round_mode == "down":
        return CRoundMode_DOWN
    elif round_mode == "up":
        return CRoundMode_UP
    elif round_mode == "towards_zero":
        return CRoundMode_TOWARDS_ZERO
    elif round_mode == "towards_infinity":
        return CRoundMode_TOWARDS_INFINITY
    elif round_mode == "half_down":
        return CRoundMode_HALF_DOWN
    elif round_mode == "half_up":
        return CRoundMode_HALF_UP
    elif round_mode == "half_towards_zero":
        return CRoundMode_HALF_TOWARDS_ZERO
    elif round_mode == "half_towards_infinity":
        return CRoundMode_HALF_TOWARDS_INFINITY
    elif round_mode == "half_to_even":
        return CRoundMode_HALF_TO_EVEN
    elif round_mode == "half_to_odd":
        return CRoundMode_HALF_TO_ODD
    _raise_invalid_function_option(round_mode, "round mode")


cdef class _RoundOptions(FunctionOptions):
    def _set_options(self, ndigits, round_mode):
        self.wrapped.reset(
            new CRoundOptions(ndigits, unwrap_round_mode(round_mode))
        )


class RoundOptions(_RoundOptions):
    """
    Options for rounding numbers.

    Parameters
    ----------
    ndigits : int, default 0
        Number of fractional digits to round to.
    round_mode : str, default "half_to_even"
        Rounding and tie-breaking mode.
        Accepted values are "down", "up", "towards_zero", "towards_infinity",
        "half_down", "half_up", "half_towards_zero", "half_towards_infinity",
        "half_to_even", "half_to_odd".
    """

    def __init__(self, ndigits=0, round_mode="half_to_even"):
        self._set_options(ndigits, round_mode)


cdef CCalendarUnit unwrap_round_temporal_unit(unit) except *:
    if unit == "nanosecond":
        return CCalendarUnit_NANOSECOND
    elif unit == "microsecond":
        return CCalendarUnit_MICROSECOND
    elif unit == "millisecond":
        return CCalendarUnit_MILLISECOND
    elif unit == "second":
        return CCalendarUnit_SECOND
    elif unit == "minute":
        return CCalendarUnit_MINUTE
    elif unit == "hour":
        return CCalendarUnit_HOUR
    elif unit == "day":
        return CCalendarUnit_DAY
    elif unit == "week":
        return CCalendarUnit_WEEK
    elif unit == "month":
        return CCalendarUnit_MONTH
    elif unit == "quarter":
        return CCalendarUnit_QUARTER
    elif unit == "year":
        return CCalendarUnit_YEAR
    _raise_invalid_function_option(unit, "Calendar unit")


cdef class _RoundTemporalOptions(FunctionOptions):
    def _set_options(self, multiple, unit, week_starts_monday):
        self.wrapped.reset(
            new CRoundTemporalOptions(
                multiple, unwrap_round_temporal_unit(unit),
                week_starts_monday)
        )


class RoundTemporalOptions(_RoundTemporalOptions):
    """
    Options for rounding temporal values.

    Parameters
    ----------
    multiple : int, default 1
        Number of units to round to.
    unit : str, default "day"
        The unit in which `multiple` is expressed.
        Accepted values are "year", "quarter", "month", "week", "day",
        "hour", "minute", "second", "millisecond", "microsecond",
        "nanosecond".
    week_starts_monday : bool, default True
        If True, weeks start on Monday; if False, on Sunday.
    """

    def __init__(self, multiple=1, unit="day", week_starts_monday=True):
        self._set_options(multiple, unit, week_starts_monday)


cdef class _RoundToMultipleOptions(FunctionOptions):
    def _set_options(self, multiple, round_mode):
        self.wrapped.reset(
            new CRoundToMultipleOptions(multiple,
                                        unwrap_round_mode(round_mode))
        )


class RoundToMultipleOptions(_RoundToMultipleOptions):
    """
    Options for rounding numbers to a multiple.

    Parameters
    ----------
    multiple : numeric scalar, default 1.0
        Multiple to round to. Should be a scalar of a type compatible
        with the argument to be rounded.
    round_mode : str, default "half_to_even"
        Rounding and tie-breaking mode.
        Accepted values are "down", "up", "towards_zero", "towards_infinity",
        "half_down", "half_up", "half_towards_zero", "half_towards_infinity",
        "half_to_even", "half_to_odd".
    """

    def __init__(self, multiple=1.0, round_mode="half_to_even"):
        self._set_options(multiple, round_mode)


cdef class _JoinOptions(FunctionOptions):
    _null_handling_map = {
        "emit_null": CJoinNullHandlingBehavior_EMIT_NULL,
        "skip": CJoinNullHandlingBehavior_SKIP,
        "replace": CJoinNullHandlingBehavior_REPLACE,
    }

    def _set_options(self, null_handling, null_replacement):
        try:
            self.wrapped.reset(
                new CJoinOptions(self._null_handling_map[null_handling],
                                 tobytes(null_replacement))
            )
        except KeyError:
            _raise_invalid_function_option(null_handling, "null handling")


class JoinOptions(_JoinOptions):
    """
    Options for the `binary_join_element_wise` function.

    Parameters
    ----------
    null_handling : str, default "emit_null"
        How to handle null values in the inputs.
        Accepted values are "emit_null", "skip", "replace".
    null_replacement : str, default ""
        Replacement string to emit for null inputs if `null_handling`
        is "replace".
    """

    def __init__(self, null_handling="emit_null", null_replacement=""):
        self._set_options(null_handling, null_replacement)


cdef class _MatchSubstringOptions(FunctionOptions):
    def _set_options(self, pattern, ignore_case):
        self.wrapped.reset(
            new CMatchSubstringOptions(tobytes(pattern), ignore_case)
        )


class MatchSubstringOptions(_MatchSubstringOptions):
    """
    Options for looking for a substring.

    Parameters
    ----------
    pattern : str
        Substring pattern to look for inside input values.
    ignore_case : bool, default False
        Whether to perform a case-insensitive match.
    """

    def __init__(self, pattern, *, ignore_case=False):
        self._set_options(pattern, ignore_case)


cdef class _PadOptions(FunctionOptions):
    def _set_options(self, width, padding):
        self.wrapped.reset(new CPadOptions(width, tobytes(padding)))


class PadOptions(_PadOptions):
    """
    Options for padding strings.

    Parameters
    ----------
    width : int
        Desired string length.
    padding : str, default " "
        What to pad the string with. Should be one byte or codepoint.
    """

    def __init__(self, width, padding=' '):
        self._set_options(width, padding)


cdef class _TrimOptions(FunctionOptions):
    def _set_options(self, characters):
        self.wrapped.reset(new CTrimOptions(tobytes(characters)))


class TrimOptions(_TrimOptions):
    """
    Options for trimming characters from strings.

    Parameters
    ----------
    characters : str
        Individual characters to be trimmed from the string.
    """

    def __init__(self, characters):
        self._set_options(tobytes(characters))


cdef class _ReplaceSubstringOptions(FunctionOptions):
    def _set_options(self, pattern, replacement, max_replacements):
        self.wrapped.reset(
            new CReplaceSubstringOptions(tobytes(pattern),
                                         tobytes(replacement),
                                         max_replacements)
        )


class ReplaceSubstringOptions(_ReplaceSubstringOptions):
    """
    Options for replacing matched substrings.

    Parameters
    ----------
    pattern : str
        Substring pattern to look for inside input values.
    replacement : str
        What to replace the pattern with.
    max_replacements : int or None, default None
        The maximum number of strings to replace in each
        input value (unlimited if None).
    """

    def __init__(self, pattern, replacement, *, max_replacements=None):
        if max_replacements is None:
            max_replacements = -1
        self._set_options(pattern, replacement, max_replacements)


cdef class _ExtractRegexOptions(FunctionOptions):
    def _set_options(self, pattern):
        self.wrapped.reset(new CExtractRegexOptions(tobytes(pattern)))


class ExtractRegexOptions(_ExtractRegexOptions):
    """
    Options for the `extract_regex` function.

    Parameters
    ----------
    pattern : str
        Regular expression with named capture fields.
    """

    def __init__(self, pattern):
        self._set_options(pattern)


cdef class _SliceOptions(FunctionOptions):
    def _set_options(self, start, stop, step):
        self.wrapped.reset(new CSliceOptions(start, stop, step))


class SliceOptions(_SliceOptions):
    """
    Options for slicing.

    Parameters
    ----------
    start : int
        Index to start slicing at (inclusive).
    stop : int or None, default None
        If given, index to stop slicing at (exclusive).
        If not given, slicing will stop at the end.
    step : int, default 1
        Slice step.
    """

    def __init__(self, start, stop=None, step=1):
        if stop is None:
            stop = sys.maxsize
        self._set_options(start, stop, step)


cdef class _ReplaceSliceOptions(FunctionOptions):
    def _set_options(self, start, stop, replacement):
        self.wrapped.reset(
            new CReplaceSliceOptions(start, stop, tobytes(replacement))
        )


class ReplaceSliceOptions(_ReplaceSliceOptions):
    """
    Options for replacing slices.

    Parameters
    ----------
    start : int
        Index to start slicing at (inclusive).
    stop : int
        Index to stop slicing at (exclusive).
    replacement : str
        What to replace the slice with.
    """

    def __init__(self, start, stop, replacement):
        self._set_options(start, stop, replacement)


cdef class _FilterOptions(FunctionOptions):
    _null_selection_map = {
        "drop": CFilterNullSelectionBehavior_DROP,
        "emit_null": CFilterNullSelectionBehavior_EMIT_NULL,
    }

    def _set_options(self, null_selection_behavior):
        try:
            self.wrapped.reset(
                new CFilterOptions(
                    self._null_selection_map[null_selection_behavior]
                )
            )
        except KeyError:
            _raise_invalid_function_option(null_selection_behavior,
                                           "null selection behavior")


class FilterOptions(_FilterOptions):
    """
    Options for selecting with a boolean filter.

    Parameters
    ----------
    null_selection_behavior : str, default "drop"
        How to handle nulls in the selection filter.
        Accepted values are "drop", "emit_null".
    """

    def __init__(self, null_selection_behavior="drop"):
        self._set_options(null_selection_behavior)


cdef class _DictionaryEncodeOptions(FunctionOptions):
    _null_encoding_map = {
        "encode": CDictionaryEncodeNullEncodingBehavior_ENCODE,
        "mask": CDictionaryEncodeNullEncodingBehavior_MASK,
    }

    def _set_options(self, null_encoding):
        try:
            self.wrapped.reset(
                new CDictionaryEncodeOptions(
                    self._null_encoding_map[null_encoding]
                )
            )
        except KeyError:
            _raise_invalid_function_option(null_encoding, "null encoding")


class DictionaryEncodeOptions(_DictionaryEncodeOptions):
    """
    Options for dictionary encoding.

    Parameters
    ----------
    null_encoding : str, default "mask"
        How to encode nulls in the input.
        Accepted values are "mask" (null inputs emit a null in the indices
        array), "encode" (null inputs emit a non-null index pointing to
        a null value in the dictionary array).
    """

    def __init__(self, null_encoding="mask"):
        self._set_options(null_encoding)


cdef class _TakeOptions(FunctionOptions):
    def _set_options(self, boundscheck):
        self.wrapped.reset(new CTakeOptions(boundscheck))


class TakeOptions(_TakeOptions):
    """
    Options for the `take` and `array_take` functions.

    Parameters
    ----------
    boundscheck : boolean, default True
        Whether to check indices are within bounds. If False and an
        index is out of boundes, behavior is undefined (the process
        may crash).
    """

    def __init__(self, *, boundscheck=True):
        self._set_options(boundscheck)


cdef class _MakeStructOptions(FunctionOptions):
    def _set_options(self, field_names, field_nullability, field_metadata):
        cdef:
            vector[c_string] c_field_names
            vector[shared_ptr[const CKeyValueMetadata]] c_field_metadata
        for name in field_names:
            c_field_names.push_back(tobytes(name))
        for metadata in field_metadata:
            c_field_metadata.push_back(pyarrow_unwrap_metadata(metadata))
        self.wrapped.reset(
            new CMakeStructOptions(c_field_names, field_nullability,
                                   c_field_metadata)
        )


class MakeStructOptions(_MakeStructOptions):
    """
    Options for the `make_struct` function.

    Parameters
    ----------
    field_names : sequence of str
        Names of the struct fields to create.
    field_nullability : sequence of bool, optional
        Nullability information for each struct field.
        If omitted, all fields are nullable.
    field_metadata : sequence of KeyValueMetadata, optional
        Metadata for each struct field.
    """

    def __init__(self, field_names=(), *, field_nullability=None,
                 field_metadata=None):
        if field_nullability is None:
            field_nullability = [True] * len(field_names)
        if field_metadata is None:
            field_metadata = [None] * len(field_names)
        self._set_options(field_names, field_nullability, field_metadata)


cdef class _StructFieldOptions(FunctionOptions):
    def _set_options(self, indices):
        self.wrapped.reset(new CStructFieldOptions(indices))


class StructFieldOptions(_StructFieldOptions):
    """
    Options for the `struct_field` function.

    Parameters
    ----------
    indices : sequence of int
        List of indices for chained field lookup, for example `[4, 1]`
        will look up the second nested field in the fifth outer field.
    """

    def __init__(self, indices):
        self._set_options(indices)


cdef class _ScalarAggregateOptions(FunctionOptions):
    def _set_options(self, skip_nulls, min_count):
        self.wrapped.reset(new CScalarAggregateOptions(skip_nulls, min_count))


class ScalarAggregateOptions(_ScalarAggregateOptions):
    __doc__ = f"""
    Options for scalar aggregations.

    Parameters
    ----------
    {_skip_nulls_doc()}
    {_min_count_doc(default=1)}
    """

    def __init__(self, *, skip_nulls=True, min_count=1):
        self._set_options(skip_nulls, min_count)


cdef class _CountOptions(FunctionOptions):
    _mode_map = {
        "only_valid": CCountMode_ONLY_VALID,
        "only_null": CCountMode_ONLY_NULL,
        "all": CCountMode_ALL,
    }

    def _set_options(self, mode):
        try:
            self.wrapped.reset(new CCountOptions(self._mode_map[mode]))
        except KeyError:
            _raise_invalid_function_option(mode, "count mode")


class CountOptions(_CountOptions):
    """
    Options for the `count` function.

    Parameters
    ----------
    mode : str, default "only_valid"
        Which values to count in the input.
        Accepted values are "only_valid", "only_null", "all".
    """

    def __init__(self, mode="only_valid"):
        self._set_options(mode)


cdef class _IndexOptions(FunctionOptions):
    def _set_options(self, scalar):
        self.wrapped.reset(new CIndexOptions(pyarrow_unwrap_scalar(scalar)))


class IndexOptions(_IndexOptions):
    """
    Options for the `index` function.

    Parameters
    ----------
    value : Scalar
        The value to search for.
    """

    def __init__(self, value):
        self._set_options(value)


cdef class _MapLookupOptions(FunctionOptions):
    _occurrence_map = {
        "all": CMapLookupOccurrence_ALL,
        "first": CMapLookupOccurrence_FIRST,
        "last": CMapLookupOccurrence_LAST,
    }

    def _set_options(self, query_key, occurrence):
        try:
            self.wrapped.reset(
                new CMapLookupOptions(
                    pyarrow_unwrap_scalar(query_key),
                    self._occurrence_map[occurrence]
                )
            )
        except KeyError:
            _raise_invalid_function_option(occurrence,
                                           "Should either be first, last, or all")


class MapLookupOptions(_MapLookupOptions):
    """
    Options for the `map_lookup` function.

    Parameters
    ----------
    query_key : Scalar
        The key to search for.
    occurrence : str
        The occurrence(s) to return from the Map
        Accepted values are "first", "last", or "all".
    """

    def __init__(self, query_key, occurrence):
        self._set_options(query_key, occurrence)


cdef class _ModeOptions(FunctionOptions):
    def _set_options(self, n, skip_nulls, min_count):
        self.wrapped.reset(new CModeOptions(n, skip_nulls, min_count))


class ModeOptions(_ModeOptions):
    __doc__ = f"""
    Options for the `mode` function.

    Parameters
    ----------
    n : int, default 1
        Number of distinct most-common values to return.
    {_skip_nulls_doc()}
    {_min_count_doc(default=0)}
    """

    def __init__(self, n=1, *, skip_nulls=True, min_count=0):
        self._set_options(n, skip_nulls, min_count)


cdef class _SetLookupOptions(FunctionOptions):
    def _set_options(self, value_set, c_bool skip_nulls):
        cdef unique_ptr[CDatum] valset
        if isinstance(value_set, Array):
            valset.reset(new CDatum((< Array > value_set).sp_array))
        elif isinstance(value_set, ChunkedArray):
            valset.reset(
                new CDatum((< ChunkedArray > value_set).sp_chunked_array)
            )
        elif isinstance(value_set, Scalar):
            valset.reset(new CDatum((< Scalar > value_set).unwrap()))
        else:
            _raise_invalid_function_option(value_set, "value set",
                                           exception_class=TypeError)

        self.wrapped.reset(new CSetLookupOptions(deref(valset), skip_nulls))


class SetLookupOptions(_SetLookupOptions):
    """
    Options for the `is_in` and `index_in` functions.

    Parameters
    ----------
    value_set : Array
        Set of values to look for in the input.
    skip_nulls : bool, default False
        If False, nulls in the input are matched in the value_set just
        like regular values.
        If True, nulls in the input always fail matching.
    """

    def __init__(self, value_set, *, skip_nulls=False):
        self._set_options(value_set, skip_nulls)


cdef class _StrptimeOptions(FunctionOptions):
    _unit_map = {
        "s": TimeUnit_SECOND,
        "ms": TimeUnit_MILLI,
        "us": TimeUnit_MICRO,
        "ns": TimeUnit_NANO,
    }

    def _set_options(self, format, unit, error_is_null):
        try:
            self.wrapped.reset(
                new CStrptimeOptions(tobytes(format), self._unit_map[unit],
                                     error_is_null)
            )
        except KeyError:
            _raise_invalid_function_option(unit, "time unit")


class StrptimeOptions(_StrptimeOptions):
    """
    Options for the `strptime` function.

    Parameters
    ----------
    format : str
        Pattern for parsing input strings as timestamps, such as "%Y/%m/%d".
    unit : str
        Timestamp unit of the output.
        Accepted values are "s", "ms", "us", "ns".
    error_is_null : boolean, default False
        Return null on parsing errors if true or raise if false.
    """

    def __init__(self, format, unit, error_is_null=False):
        self._set_options(format, unit, error_is_null)


cdef class _StrftimeOptions(FunctionOptions):
    def _set_options(self, format, locale):
        self.wrapped.reset(
            new CStrftimeOptions(tobytes(format), tobytes(locale))
        )


class StrftimeOptions(_StrftimeOptions):
    """
    Options for the `strftime` function.

    Parameters
    ----------
    format : str, default "%Y-%m-%dT%H:%M:%S"
        Pattern for formatting input values.
    locale : str, default "C"
        Locale to use for locale-specific format specifiers.
    """

    def __init__(self, format="%Y-%m-%dT%H:%M:%S", locale="C"):
        self._set_options(format, locale)


cdef class _DayOfWeekOptions(FunctionOptions):
    def _set_options(self, count_from_zero, week_start):
        self.wrapped.reset(
            new CDayOfWeekOptions(count_from_zero, week_start)
        )


class DayOfWeekOptions(_DayOfWeekOptions):
    """
    Options for the `day_of_week` function.

    Parameters
    ----------
    count_from_zero : bool, default True
        If True, number days from 0, otherwise from 1.
    week_start : int, default 1
        Which day does the week start with (Monday=1, Sunday=7).
        How this value is numbered is unaffected by `count_from_zero`.
    """

    def __init__(self, *, count_from_zero=True, week_start=1):
        self._set_options(count_from_zero, week_start)


cdef class _WeekOptions(FunctionOptions):
    def _set_options(self, week_starts_monday, count_from_zero,
                     first_week_is_fully_in_year):
        self.wrapped.reset(
            new CWeekOptions(week_starts_monday, count_from_zero,
                             first_week_is_fully_in_year)
        )


class WeekOptions(_WeekOptions):
    """
    Options for the `week` function.

    Parameters
    ----------
    week_starts_monday : bool, default True
        If True, weeks start on Monday; if False, on Sunday.
    count_from_zero : bool, default False
        If True, dates at the start of a year that fall into the last week
        of the previous year emit 0.
        If False, they emit 52 or 53 (the week number of the last week
        of the previous year).
    first_week_is_fully_in_year : bool, default False
        If True, week number 0 is fully in January.
        If False, a week that begins on December 29, 30 or 31 is considered
        to be week number 0 of the following year.
    """

    def __init__(self, *, week_starts_monday=True, count_from_zero=False,
                 first_week_is_fully_in_year=False):
        self._set_options(week_starts_monday,
                          count_from_zero, first_week_is_fully_in_year)


cdef class _AssumeTimezoneOptions(FunctionOptions):
    _ambiguous_map = {
        "raise": CAssumeTimezoneAmbiguous_AMBIGUOUS_RAISE,
        "earliest": CAssumeTimezoneAmbiguous_AMBIGUOUS_EARLIEST,
        "latest": CAssumeTimezoneAmbiguous_AMBIGUOUS_LATEST,
    }
    _nonexistent_map = {
        "raise": CAssumeTimezoneNonexistent_NONEXISTENT_RAISE,
        "earliest": CAssumeTimezoneNonexistent_NONEXISTENT_EARLIEST,
        "latest": CAssumeTimezoneNonexistent_NONEXISTENT_LATEST,
    }

    def _set_options(self, timezone, ambiguous, nonexistent):
        if ambiguous not in self._ambiguous_map:
            _raise_invalid_function_option(ambiguous,
                                           "'ambiguous' timestamp handling")
        if nonexistent not in self._nonexistent_map:
            _raise_invalid_function_option(nonexistent,
                                           "'nonexistent' timestamp handling")
        self.wrapped.reset(
            new CAssumeTimezoneOptions(tobytes(timezone),
                                       self._ambiguous_map[ambiguous],
                                       self._nonexistent_map[nonexistent])
        )


class AssumeTimezoneOptions(_AssumeTimezoneOptions):
    """
    Options for the `assume_timezone` function.

    Parameters
    ----------
    timezone : str
        Timezone to assume for the input.
    ambiguous : str, default "raise"
        How to handle timestamps that are ambiguous in the assumed timezone.
        Accepted values are "raise", "earliest", "latest".
    nonexistent : str, default "raise"
        How to handle timestamps that don't exist in the assumed timezone.
        Accepted values are "raise", "earliest", "latest".
    """

    def __init__(self, timezone, *, ambiguous="raise", nonexistent="raise"):
        self._set_options(timezone, ambiguous, nonexistent)


cdef class _NullOptions(FunctionOptions):
    def _set_options(self, nan_is_null):
        self.wrapped.reset(new CNullOptions(nan_is_null))


class NullOptions(_NullOptions):
    """
    Options for the `is_null` function.

    Parameters
    ----------
    nan_is_null : bool, default False
        Whether floating-point NaN values are considered null.
    """

    def __init__(self, *, nan_is_null=False):
        self._set_options(nan_is_null)


cdef class _VarianceOptions(FunctionOptions):
    def _set_options(self, ddof, skip_nulls, min_count):
        self.wrapped.reset(new CVarianceOptions(ddof, skip_nulls, min_count))


class VarianceOptions(_VarianceOptions):
    __doc__ = f"""
    Options for the `variance` and `stddev` functions.

    Parameters
    ----------
    ddof : int, default 0
        Number of degrees of freedom.
    {_skip_nulls_doc()}
    {_min_count_doc(default=0)}
    """

    def __init__(self, *, ddof=0, skip_nulls=True, min_count=0):
        self._set_options(ddof, skip_nulls, min_count)


cdef class _SplitOptions(FunctionOptions):
    def _set_options(self, max_splits, reverse):
        self.wrapped.reset(new CSplitOptions(max_splits, reverse))


class SplitOptions(_SplitOptions):
    """
    Options for splitting on whitespace.

    Parameters
    ----------
    max_splits : int or None, default None
        Maximum number of splits for each input value (unlimited if None).
    reverse : bool, default False
        Whether to start splitting from the end of each input value.
        This only has an effect if `max_splits` is not None.
    """

    def __init__(self, *, max_splits=None, reverse=False):
        if max_splits is None:
            max_splits = -1
        self._set_options(max_splits, reverse)


cdef class _SplitPatternOptions(FunctionOptions):
    def _set_options(self, pattern, max_splits, reverse):
        self.wrapped.reset(
            new CSplitPatternOptions(tobytes(pattern), max_splits, reverse)
        )


class SplitPatternOptions(_SplitPatternOptions):
    """
    Options for splitting on a string pattern.

    Parameters
    ----------
    pattern : str
        String pattern to split on.
    max_splits : int or None, default None
        Maximum number of splits for each input value (unlimited if None).
    reverse : bool, default False
        Whether to start splitting from the end of each input value.
        This only has an effect if `max_splits` is not None.
    """

    def __init__(self, pattern, *, max_splits=None, reverse=False):
        if max_splits is None:
            max_splits = -1
        self._set_options(pattern, max_splits, reverse)


cdef CSortOrder unwrap_sort_order(order) except *:
    if order == "ascending":
        return CSortOrder_Ascending
    elif order == "descending":
        return CSortOrder_Descending
    _raise_invalid_function_option(order, "sort order")


cdef CNullPlacement unwrap_null_placement(null_placement) except *:
    if null_placement == "at_start":
        return CNullPlacement_AtStart
    elif null_placement == "at_end":
        return CNullPlacement_AtEnd
    _raise_invalid_function_option(null_placement, "null placement")


cdef class _PartitionNthOptions(FunctionOptions):
    def _set_options(self, pivot, null_placement):
        self.wrapped.reset(new CPartitionNthOptions(
            pivot, unwrap_null_placement(null_placement)))


class PartitionNthOptions(_PartitionNthOptions):
    """
    Options for the `partition_nth_indices` function.

    Parameters
    ----------
    pivot : int
        Index into the equivalent sorted array of the pivot element.
    null_placement : str, default "at_end"
        Where nulls in the input should be partitioned.
        Accepted values are "at_start", "at_end".
    """

    def __init__(self, pivot, *, null_placement="at_end"):
        self._set_options(pivot, null_placement)


cdef class _ArraySortOptions(FunctionOptions):
    def _set_options(self, order, null_placement):
        self.wrapped.reset(new CArraySortOptions(
            unwrap_sort_order(order), unwrap_null_placement(null_placement)))


class ArraySortOptions(_ArraySortOptions):
    """
    Options for the `array_sort_indices` function.

    Parameters
    ----------
    order : str, default "ascending"
        Which order to sort values in.
        Accepted values are "ascending", "descending".
    null_placement : str, default "at_end"
        Where nulls in the input should be sorted.
        Accepted values are "at_start", "at_end".
    """

    def __init__(self, order="ascending", *, null_placement="at_end"):
        self._set_options(order, null_placement)


cdef class _SortOptions(FunctionOptions):
    def _set_options(self, sort_keys, null_placement):
        cdef vector[CSortKey] c_sort_keys
        for name, order in sort_keys:
            c_sort_keys.push_back(
                CSortKey(tobytes(name), unwrap_sort_order(order))
            )
        self.wrapped.reset(new CSortOptions(
            c_sort_keys, unwrap_null_placement(null_placement)))


class SortOptions(_SortOptions):
    """
    Options for the `sort_indices` function.

    Parameters
    ----------
    sort_keys : sequence of (name, order) tuples
        Names of field/column keys to sort the input on,
        along with the order each field/column is sorted in.
        Accepted values for `order` are "ascending", "descending".
    null_placement : str, default "at_end"
        Where nulls in input should be sorted, only applying to
        columns/fields mentioned in `sort_keys`.
        Accepted values are "at_start", "at_end".
    """

    def __init__(self, sort_keys=(), *, null_placement="at_end"):
        self._set_options(sort_keys, null_placement)


cdef class _SelectKOptions(FunctionOptions):
    def _set_options(self, k, sort_keys):
        cdef vector[CSortKey] c_sort_keys
        for name, order in sort_keys:
            c_sort_keys.push_back(
                CSortKey(tobytes(name), unwrap_sort_order(order))
            )
        self.wrapped.reset(new CSelectKOptions(k, c_sort_keys))


class SelectKOptions(_SelectKOptions):
    """
    Options for top/bottom k-selection.

    Parameters
    ----------
    k : int
        Number of leading values to select in sorted order
        (i.e. the largest values if sort order is "descending",
        the smallest otherwise).
    sort_keys : sequence of (name, order) tuples
        Names of field/column keys to sort the input on,
        along with the order each field/column is sorted in.
        Accepted values for `order` are "ascending", "descending".
    """

    def __init__(self, k, sort_keys):
        self._set_options(k, sort_keys)


cdef class _QuantileOptions(FunctionOptions):
    _interp_map = {
        "linear": CQuantileInterp_LINEAR,
        "lower": CQuantileInterp_LOWER,
        "higher": CQuantileInterp_HIGHER,
        "nearest": CQuantileInterp_NEAREST,
        "midpoint": CQuantileInterp_MIDPOINT,
    }

    def _set_options(self, quantiles, interp, skip_nulls, min_count):
        try:
            self.wrapped.reset(
                new CQuantileOptions(quantiles, self._interp_map[interp],
                                     skip_nulls, min_count)
            )
        except KeyError:
            _raise_invalid_function_option(interp, "quantile interpolation")


class QuantileOptions(_QuantileOptions):
    __doc__ = f"""
    Options for the `quantile` function.

    Parameters
    ----------
    q : double or sequence of double, default 0.5
        Quantiles to compute. All values must be in [0, 1].
    interpolation : str, default "linear"
        How to break ties between competing data points for a given quantile.
        Accepted values are:

        - "linear": compute an interpolation
        - "lower": always use the smallest of the two data points
        - "higher": always use the largest of the two data points
        - "nearest": select the data point that is closest to the quantile
        - "midpoint": compute the (unweighted) mean of the two data points
    {_skip_nulls_doc()}
    {_min_count_doc(default=0)}
    """

    def __init__(self, q=0.5, *, interpolation="linear", skip_nulls=True,
                 min_count=0):
        if not isinstance(q, (list, tuple, np.ndarray)):
            q = [q]
        self._set_options(q, interpolation, skip_nulls, min_count)


cdef class _TDigestOptions(FunctionOptions):
    def _set_options(self, quantiles, delta, buffer_size, skip_nulls,
                     min_count):
        self.wrapped.reset(
            new CTDigestOptions(quantiles, delta, buffer_size, skip_nulls,
                                min_count)
        )


class TDigestOptions(_TDigestOptions):
    __doc__ = f"""
    Options for the `tdigest` function.

    Parameters
    ----------
    q : double or sequence of double, default 0.5
        Quantiles to approximate. All values must be in [0, 1].
    delta : int, default 100
        Compression parameter for the T-digest algorithm.
    buffer_size : int, default 500
        Buffer size for the T-digest algorithm.
    {_skip_nulls_doc()}
    {_min_count_doc(default=0)}
    """

    def __init__(self, q=0.5, *, delta=100, buffer_size=500, skip_nulls=True,
                 min_count=0):
        if not isinstance(q, (list, tuple, np.ndarray)):
            q = [q]
        self._set_options(q, delta, buffer_size, skip_nulls, min_count)


cdef class _Utf8NormalizeOptions(FunctionOptions):
    _form_map = {
        "NFC": CUtf8NormalizeForm_NFC,
        "NFKC": CUtf8NormalizeForm_NFKC,
        "NFD": CUtf8NormalizeForm_NFD,
        "NFKD": CUtf8NormalizeForm_NFKD,
    }

    def _set_options(self, form):
        try:
            self.wrapped.reset(
                new CUtf8NormalizeOptions(self._form_map[form])
            )
        except KeyError:
            _raise_invalid_function_option(form,
                                           "Unicode normalization form")


class Utf8NormalizeOptions(_Utf8NormalizeOptions):
    """
    Options for the `utf8_normalize` function.

    Parameters
    ----------
    form : str
        Unicode normalization form.
        Accepted values are "NFC", "NFKC", "NFD", NFKD".
    """

    def __init__(self, form):
        self._set_options(form)


cdef class _RandomOptions(FunctionOptions):
    def _set_options(self, length, initializer):
        if initializer == 'system':
            self.wrapped.reset(new CRandomOptions(
                CRandomOptions.FromSystemRandom(length)))
            return

        if not isinstance(initializer, int):
            try:
                initializer = hash(initializer)
            except TypeError:
                raise TypeError(
                    f"initializer should be 'system', an integer, "
                    f"or a hashable object; got {initializer!r}")

        if initializer < 0:
            initializer += 2**64
        self.wrapped.reset(new CRandomOptions(
            CRandomOptions.FromSeed(length, initializer)))


class RandomOptions(_RandomOptions):
    """
    Options for random generation.

    Parameters
    ----------
    length : int
        Number of random values to generate.
    initializer : int or str
        How to initialize the underlying random generator.
        If an integer is given, it is used as a seed.
        If "system" is given, the random generator is initialized with
        a system-specific source of (hopefully true) randomness.
        Other values are invalid.
    """

    def __init__(self, length, *, initializer='system'):
        self._set_options(length, initializer)


def _group_by(args, keys, aggregations):
    cdef:
        vector[CDatum] c_args
        vector[CDatum] c_keys
        vector[CAggregate] c_aggregations
        CDatum result
        CAggregate c_aggr

    _pack_compute_args(args, &c_args)
    _pack_compute_args(keys, &c_keys)

    for aggr_func_name, aggr_opts in aggregations:
        c_aggr.function = tobytes(aggr_func_name)
        if aggr_opts is not None:
            c_aggr.options = (<FunctionOptions?> aggr_opts).get_options()
        else:
            c_aggr.options = NULL
        c_aggregations.push_back(c_aggr)

    with nogil:
        result = GetResultValue(
            GroupBy(c_args, c_keys, c_aggregations)
        )

    return wrap_datum(result)


cdef class Expression(_Weakrefable):
    """
    A logical expression to be evaluated against some input.

    To create an expression:

    - Use the factory function ``pyarrow.compute.scalar()`` to create a
      scalar (not necessary when combined, see example below).
    - Use the factory function ``pyarrow.compute.field()`` to reference
      a field (column in table).
    - Compare fields and scalars with ``<``, ``<=``, ``==``, ``>=``, ``>``.
    - Combine expressions using python operators ``&`` (logical and),
      ``|`` (logical or) and ``~`` (logical not).
      Note: python keywords ``and``, ``or`` and ``not`` cannot be used
      to combine expressions.
    - Check whether the expression is contained in a list of values with
      the ``pyarrow.compute.Expression.isin()`` member function.

    Examples
    --------

    >>> import pyarrow.compute as pc
    >>> (pc.field("a") < pc.scalar(3)) | (pc.field("b") > 7)
    <pyarrow.compute.Expression ((a < 3:int64) or (b > 7:int64))>
    >>> ds.field('a') != 3
    <pyarrow.compute.Expression (a != 3)>
    >>> ds.field('a').isin([1, 2, 3])
    <pyarrow.compute.Expression (a is in [
      1,
      2,
      3
    ])>
    """

    def __init__(self):
        msg = 'Expression is an abstract class thus cannot be initialized.'
        raise TypeError(msg)

    cdef void init(self, const CExpression& sp):
        self.expr = sp

    @staticmethod
    cdef wrap(const CExpression& sp):
        cdef Expression self = Expression.__new__(Expression)
        self.init(sp)
        return self

    cdef inline CExpression unwrap(self):
        return self.expr

    def equals(self, Expression other):
        return self.expr.Equals(other.unwrap())

    def __str__(self):
        return frombytes(self.expr.ToString())

    def __repr__(self):
        return "<pyarrow.compute.{0} {1}>".format(
            self.__class__.__name__, str(self)
        )

    @staticmethod
    def _deserialize(Buffer buffer not None):
        return Expression.wrap(GetResultValue(CDeserializeExpression(
            pyarrow_unwrap_buffer(buffer))))

    def __reduce__(self):
        buffer = pyarrow_wrap_buffer(GetResultValue(
            CSerializeExpression(self.expr)))
        return Expression._deserialize, (buffer,)

    @staticmethod
    cdef Expression _expr_or_scalar(object expr):
        if isinstance(expr, Expression):
            return (<Expression> expr)
        return (<Expression> Expression._scalar(expr))

    @staticmethod
    def _call(str function_name, list arguments, FunctionOptions options=None):
        cdef:
            vector[CExpression] c_arguments
            shared_ptr[CFunctionOptions] c_options

        for argument in arguments:
            if not isinstance(argument, Expression):
                raise TypeError("only other expressions allowed as arguments")
            c_arguments.push_back((<Expression> argument).expr)

        if options is not None:
            c_options = options.unwrap()

        return Expression.wrap(CMakeCallExpression(
            tobytes(function_name), move(c_arguments), c_options))

    def __richcmp__(self, other, int op):
        other = Expression._expr_or_scalar(other)
        return Expression._call({
            Py_EQ: "equal",
            Py_NE: "not_equal",
            Py_GT: "greater",
            Py_GE: "greater_equal",
            Py_LT: "less",
            Py_LE: "less_equal",
        }[op], [self, other])

    def __bool__(self):
        raise ValueError(
            "An Expression cannot be evaluated to python True or False. "
            "If you are using the 'and', 'or' or 'not' operators, use '&', "
            "'|' or '~' instead."
        )

    def __invert__(self):
        return Expression._call("invert", [self])

    def __and__(Expression self, other):
        other = Expression._expr_or_scalar(other)
        return Expression._call("and_kleene", [self, other])

    def __or__(Expression self, other):
        other = Expression._expr_or_scalar(other)
        return Expression._call("or_kleene", [self, other])

    def __add__(Expression self, other):
        other = Expression._expr_or_scalar(other)
        return Expression._call("add_checked", [self, other])

    def __mul__(Expression self, other):
        other = Expression._expr_or_scalar(other)
        return Expression._call("multiply_checked", [self, other])

    def __sub__(Expression self, other):
        other = Expression._expr_or_scalar(other)
        return Expression._call("subtract_checked", [self, other])

    def __truediv__(Expression self, other):
        other = Expression._expr_or_scalar(other)
        return Expression._call("divide_checked", [self, other])

    def is_valid(self):
        """Checks whether the expression is not-null (valid)"""
        return Expression._call("is_valid", [self])

    def is_null(self, bint nan_is_null=False):
        """Checks whether the expression is null"""
        options = NullOptions(nan_is_null=nan_is_null)
        return Expression._call("is_null", [self], options)

    def cast(self, type, bint safe=True):
        """Explicitly change the expression's data type"""
        options = CastOptions.safe(ensure_type(type))
        return Expression._call("cast", [self], options)

    def isin(self, values):
        """Checks whether the expression is contained in values"""
        if not isinstance(values, Array):
            values = lib.array(values)

        options = SetLookupOptions(values)
        return Expression._call("is_in", [self], options)

    @staticmethod
    def _field(str name not None):
        return Expression.wrap(CMakeFieldExpression(tobytes(name)))

    @staticmethod
    def _scalar(value):
        cdef:
            Scalar scalar

        if isinstance(value, Scalar):
            scalar = value
        else:
            scalar = lib.scalar(value)

        return Expression.wrap(CMakeScalarExpression(scalar.unwrap()))


_deserialize = Expression._deserialize
cdef CExpression _true = CMakeScalarExpression(
    <shared_ptr[CScalar]> make_shared[CBooleanScalar](True)
)


cdef CExpression _bind(Expression filter, Schema schema) except *:
    assert schema is not None

    if filter is None:
        return _true

    return GetResultValue(filter.unwrap().Bind(
        deref(pyarrow_unwrap_schema(schema).get())))

cdef CFunctionDoc _make_function_doc(func_doc):
    cdef:
        CFunctionDoc f_doc
        vector[c_string] c_arg_names
    if isinstance(func_doc, dict):
        if func_doc["summary"] and isinstance(func_doc["summary"], str): 
            f_doc.summary = func_doc["summary"].encode() 
        else: 
            raise ValueError("key `summary` cannot be None")
        
        if func_doc["description"] and isinstance(func_doc["description"], str): 
            f_doc.description = func_doc["description"].encode() 
        else: 
            raise ValueError("key `description` cannot be None")
        
        if func_doc["arg_names"] and isinstance(func_doc["arg_names"], list): 
            for arg_name in func_doc["arg_names"]:
                if isinstance(arg_name, str):
                    c_arg_names.push_back(arg_name.encode())
                else:
                    raise ValueError("key `arg_names` must be a list of strings")
            f_doc.arg_names = c_arg_names
        else: 
            raise ValueError("key `arg_names` cannot be None")
        
        if func_doc["options_class"] and isinstance(func_doc["options_class"], str): 
            f_doc.options_class = func_doc["options_class"].encode()
        else: 
            raise ValueError("key `options_class` cannot be None")
        
        if isinstance(func_doc["options_required"], bool): 
            f_doc.options_required = func_doc["options_required"]
        else: 
            raise ValueError("key `options_required` cannot must be bool")

        return f_doc
    else:
        raise TypeError(f"func_doc must be a dictionary")

cdef object py_function = None

def py_function(arrow_array):
    p_new_array = call_function("add", [arrow_array, 1])
    return p_new_array

cdef CStatus udf(self, CKernelContext* ctx, const CExecBatch& batch, CDatum* out) nogil:
    cdef CDatum datum = batch.values[0]
    cdef shared_ptr[CArrayData] array_data = datum.array()
    cdef shared_ptr[CArray] c_array = MakeArray(array_data)
    cdef shared_ptr[CArray] new_array
    with gil:
        p_array = pyarrow_wrap_array(c_array)
        new_array = pyarrow_unwrap_array(py_function(p_array))
    cdef CDatum new_datum = CDatum(new_array)
    out[0] = new_datum
    return CStatus_OK()

def register_function(func_name, arity, function_doc, in_types, out_type, callback):
        cdef:
            c_string c_func_name
            CArity c_arity
            CFunctionDoc c_func_doc
            CInputType in_tmp
            vector[CInputType] c_in_types
            ExecFunc c_callback
            shared_ptr[CDataType] c_type
        
        if func_name and isinstance(func_name, str):
            c_func_name = func_name.encode()
        else:
            raise ValueError("func_name should be str")
        
        if arity and isinstance(arity, Arity):
            c_arity = (<Arity> arity).arity
        else:
            raise ValueError("arity must be an instance of Arity")
        
        c_func_doc = _make_function_doc(function_doc)

        if in_types and isinstance(in_types, list):
            for in_type in in_types:
                in_tmp = (<InputType> in_type).input_type
                c_in_types.push_back(in_tmp)

        c_type = pyarrow_unwrap_data_type(out_type)
        c_callback = <ExecFunc>udf
        cdef COutputType* c_out_type = new COutputType(c_type)
        cdef CUDFSynthesizer* c_udf_syn = new CUDFSynthesizer(c_func_name, 
            c_arity, c_func_doc, c_in_types, deref(c_out_type), c_callback)
        c_udf_syn.MakeFunction()

def register_pyfunction(func_name, arity, function_doc, in_types, out_type, callback):
        cdef:
            c_string c_func_name
            CArity c_arity
            CFunctionDoc c_func_doc
            CInputType in_tmp
            vector[CInputType] c_in_types
            PyObject* c_callback
            shared_ptr[CDataType] c_type
            object obj
        
        if func_name and isinstance(func_name, str):
            c_func_name = func_name.encode()
        else:
            raise ValueError("func_name should be str")
        
        if arity and isinstance(arity, Arity):
            c_arity = (<Arity> arity).arity
        else:
            raise ValueError("arity must be an instance of Arity")
        
        c_func_doc = _make_function_doc(function_doc)

        if in_types and isinstance(in_types, list):
            for in_type in in_types:
                in_tmp = (<InputType> in_type).input_type
                c_in_types.push_back(in_tmp)

        c_type = pyarrow_unwrap_data_type(out_type)
        c_callback = <PyObject*>callback
        #c_callback = <ExecFunc>udf
        cdef COutputType* c_out_type = new COutputType(c_type)
        cdef CUDFSynthesizer* c_udf_syn = new CUDFSynthesizer(c_func_name, 
            c_arity, c_func_doc, c_in_types, deref(c_out_type))
        c_udf_syn.MakePyFunction(c_callback)
