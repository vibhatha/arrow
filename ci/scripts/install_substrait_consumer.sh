#!/usr/bin/env bash
#
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

set -e

echo "INSTALL SUBSTRAIT CONSUMER TEST SUITE";

git clone https://github.com/substrait-io/consumer-testing.git
cd consumer-testing
# avoid installing pyarrow
cat requirements.txt | while read line
do
    if [ $line != "pyarrow" ]; then
        pip install $line
    fi
done

pip install -r requirements-build.txt
# TODO: write a better installation and testing script
python setup.py install
echo ">>>>>>>>>>>>>>>> consumer-testing installed !!! @ install_substrait_consumer.sh"
python -c "import pyarrow.substrait"
python -c "from substrait_consumer.consumers import AceroConsumer"

pytest tests/integration/test_acero_tpch.py

echo ">>>>>>>>>>>>>>>> ALL DONE!!! @ install_substrait_consumer.sh"
