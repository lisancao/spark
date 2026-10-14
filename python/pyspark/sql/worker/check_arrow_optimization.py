#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from typing import IO

from pyspark.serializers import write_int, write_with_length
from pyspark.sql.pandas.utils import require_minimum_pandas_version, require_minimum_pyarrow_version
from pyspark.sql.worker.utils import worker_run
from pyspark.worker_util import get_sock_file_to_executor


def _main(infile: IO, outfile: IO) -> None:
    """
    Main method for checking whether this Python can run Arrow-optimized Python UDFs.

    This process is invoked from the `ArrowOptimizationCheckRunner.runInPython` method in JVM.
    A Python client runs this check before it creates an Arrow-optimized Python UDF, and creates
    a regular one if it fails, see `_create_py_udf` in `pyspark.sql.udf`. A Spark Connect client
    runs it in its own Python, but the UDF runs in the Python on the server.

    This is responsible for running the same check, the minimum versions of pandas and PyArrow,
    in the Python that runs the UDFs, and for sending back why it failed if it did.
    """
    try:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
    except ImportError as e:
        write_int(0, outfile)
        write_with_length(str(e).encode("utf-8"), outfile)
    else:
        write_int(1, outfile)


def main(infile: IO, outfile: IO) -> None:
    worker_run(_main, infile, outfile)


if __name__ == "__main__":
    with get_sock_file_to_executor() as sock_file:
        main(sock_file, sock_file)
