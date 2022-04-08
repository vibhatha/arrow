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

from pyarrow import Buffer
from pyarrow.lib cimport *
from pyarrow.includes.libarrow cimport *


def run_query(plan):
    """
    Executes a substrait plan and returns a RecordBatchReader.

    Parameters
    ----------
    plan : bytes or Buffer
        Substrait Plan can be fed as a encoded string in utf-8
        as a JSON string or as an Arrow Buffer. 
    """

    cdef:
        CResult[shared_ptr[CRecordBatchReader]] c_res_reader
        shared_ptr[CRecordBatchReader] c_reader
        RecordBatchReader reader
        c_string c_str_plan
        shared_ptr[CBuffer] c_buf_plan

    if isinstance(plan, bytes):
        c_str_plan = plan
        c_res_reader = GetRecordBatchReader(c_str_plan)
    elif isinstance(plan, Buffer):
        c_buf_plan = pyarrow_unwrap_buffer(plan)
        c_res_reader = GetRecordBatchReader(c_buf_plan)
    else:
        raise ValueError("Expected bytes or pyarrow.Buffer")

    c_reader = GetResultValue(c_res_reader)

    reader = RecordBatchReader.__new__(RecordBatchReader)
    reader.reader = c_reader
    return reader


def get_buffer_from_json(plan):
    """
    Returns Buffer object by converting substrait plan in 
    JSON.

    Parameter
    ---------
    plan: byte
        Substrait plan as a bytes.
    """

    cdef:
        CResult[shared_ptr[CBuffer]] c_res_buffer
        c_string c_str_plan
        shared_ptr[CBuffer] c_buf_plan

    if isinstance(plan, bytes):
        c_str_plan = plan
        c_res_buffer = GetSubstraitBufferFromJSON(c_str_plan)
        c_buf_plan = GetResultValue(c_res_buffer)
    else:
        raise ValueError("Expected plan in bytes.")
    return pyarrow_wrap_buffer(c_buf_plan)
