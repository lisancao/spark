/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.python

import java.io.{DataInputStream, DataOutputStream}

import net.razorvine.pickle.Pickler

import org.apache.spark.api.python.{PythonException, PythonFunction, PythonWorkerUtils, SpecialLengths}

/**
 * Checks whether the Python that runs `func` can run Arrow-optimized Python UDFs, that is,
 * whether pandas and PyArrow are installed at the minimum versions they require.
 *
 * A Python client runs this check before it creates an Arrow-optimized Python UDF, and creates a
 * regular one instead if the check fails. A Spark Connect client runs it in its own Python, but
 * the UDF runs in the Python on the server, so this runs the same check in that Python.
 *
 * Returns `None` if the check passes, and why it failed otherwise.
 */
private[python] class ArrowOptimizationCheckRunner(func: PythonFunction)
    extends PythonPlannerRunner[Option[String]](func) {

  override val workerModule = "pyspark.sql.worker.check_arrow_optimization"

  override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
    // No input needed.
  }

  override protected def receiveFromPython(dataIn: DataInputStream): Option[String] = {
    // Receive whether the check passed, or an exception raised in Python worker.
    val passed = dataIn.readInt()
    if (passed == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      throw new PythonException(
        errorClass = "PYTHON_EXCEPTION",
        messageParameters = Map(
          "msg" -> "An exception was thrown from the Python worker",
          "traceback" -> PythonWorkerUtils.readUTF(dataIn)))
    }
    if (passed != 0) None else Some(PythonWorkerUtils.readUTF(dataIn))
  }
}
