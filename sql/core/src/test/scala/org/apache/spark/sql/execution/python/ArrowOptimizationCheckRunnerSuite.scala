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

import java.io.File
import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.api.python.{PythonException, PythonUtils, SimplePythonFunction}
import org.apache.spark.sql.{IntegratedUDFTestUtils, QueryTest}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class ArrowOptimizationCheckRunnerSuite extends QueryTest with SharedSparkSession {
  import IntegratedUDFTestUtils._

  private lazy val testPythonPath = PythonUtils.mergePythonPaths(pysparkPythonPath, pythonPath)

  private def pythonFunction(
      pythonPath: String,
      pythonVer: String = IntegratedUDFTestUtils.pythonVer): SimplePythonFunction = {
    SimplePythonFunction(
      command = Seq.empty[Byte],
      envVars = new java.util.HashMap[String, String](Map("PYTHONPATH" -> pythonPath).asJava),
      pythonIncludes = new java.util.ArrayList[String](),
      pythonExec = pythonExec,
      pythonVer = pythonVer,
      broadcastVars = new java.util.ArrayList(),
      accumulator = null)
  }

  /**
   * Runs the check in a Python worker that imports `modules`, each a module name and its source,
   * in place of the installed ones.
   */
  private def check(modules: (String, String)*): Option[String] = {
    val dir = Utils.createTempDir()
    try {
      modules.foreach { case (name, source) =>
        Files.writeString(new File(dir, s"$name.py").toPath, source)
      }
      val pythonPath = PythonUtils.mergePythonPaths(dir.getAbsolutePath, testPythonPath)
      new ArrowOptimizationCheckRunner(pythonFunction(pythonPath)).runInPython()
    } finally {
      Utils.deleteRecursively(dir)
    }
  }

  /** The source of a module that fails to import, as if it were not installed. */
  private def notInstalled(name: String): String =
    s"""raise ModuleNotFoundError("No module named '$name'", name="$name")"""

  test("SPARK-60241: passes with the installed pandas and PyArrow") {
    assume(shouldTestPandasUDFs)
    assert(check() === None)
  }

  test("SPARK-60241: fails when pandas is not installed") {
    assume(shouldTestPythonUDFs)
    val reason = check("pandas" -> notInstalled("pandas"))
    assert(reason.exists(_.contains("[PACKAGE_NOT_INSTALLED] Pandas >= ")), reason)
  }

  test("SPARK-60241: fails when pandas is older than the minimum version") {
    assume(shouldTestPythonUDFs)
    val reason = check("pandas" -> "__version__ = \"1.0.0\"")
    assert(reason.exists(_.contains("[UNSUPPORTED_PACKAGE_VERSION] Pandas >= ")), reason)
  }

  test("SPARK-60241: fails when PyArrow is not installed") {
    assume(shouldTestPandasUDFs)
    val reason = check("pyarrow" -> notInstalled("pyarrow"))
    assert(reason.exists(_.contains("[PACKAGE_NOT_INSTALLED] PyArrow >= ")), reason)
  }

  test("SPARK-60241: throws when the Python worker fails") {
    assume(shouldTestPythonUDFs)
    val e = intercept[PythonException] {
      new ArrowOptimizationCheckRunner(pythonFunction(testPythonPath, pythonVer = "0.0"))
        .runInPython()
    }
    assert(e.getCondition === "PYTHON_EXCEPTION")
    assert(e.getMessage.contains("PYTHON_VERSION_MISMATCH"))
  }
}
