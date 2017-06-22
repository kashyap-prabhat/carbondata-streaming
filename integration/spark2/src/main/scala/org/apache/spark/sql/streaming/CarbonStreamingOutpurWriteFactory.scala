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

package org.apache.spark.sql.streaming

import java.io.{BufferedWriter, FileWriter, IOException}
import java.util.concurrent.ConcurrentHashMap

//import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.types.StructType


class CarbonStreamingOutputWriterFactory extends OutputWriterFactory {

  def writeLog(message: String): Unit = {
    try {
      val bw = new BufferedWriter(new FileWriter("/home/prabhat/test.log", true))
      try {
        bw.append("[CarbonStreamingOutputWriterFactory]" + message).append("\n")
        Console.print("[CarbonStreamingOutputWriterFactory]" + message)
      } catch {
        case e: IOException =>
          e.printStackTrace()
      } finally if (bw != null) bw.close()
    }
  }

  /**
    * When writing to a [[org.apache.spark.sql.execution.datasources.HadoopFsRelation]], this method gets called by each task on executor side
    * to instantiate new [[org.apache.spark.sql.execution.datasources.OutputWriter]]s.
    *
    * @param path Path to write the file.
    * @param dataSchema Schema of the rows to be written. Partition columns are not included in the
    *        schema if the relation being written is partitioned.
    * @param context The Hadoop MapReduce task context.
    */


  override def newInstance(
                            path: String,

                            dataSchema: StructType,

                            context: TaskAttemptContext) : CarbonStreamingOutputWriter = {

    writeLog("Inside new Instance")
    new CarbonStreamingOutputWriter(path, context)
  }

  override def getFileExtension(context: TaskAttemptContext): String = {

    CarbonTablePath.STREAM_FILE_NAME_EXT
  }

}

object CarbonStreamingOutpurWriterFactory {

  def writeLog(message: String): Unit = {
    try {
      val bw = new BufferedWriter(new FileWriter("/home/prabhat/test.log", true))
      try {
        bw.append("[CarbonStreamingOutputWriterFactory]" + message).append("\n")
        Console.print("[CarbonStreamingOutputWriterFactory]" + message)
      } catch {
        case e: IOException =>
          e.printStackTrace()
      } finally if (bw != null) bw.close()
    }
  }


  private[this] val writers = new ConcurrentHashMap[String, CarbonStreamingOutputWriter]()

  def addWriter(path: String, writer: CarbonStreamingOutputWriter): Unit = {
    writeLog("[Add Writer] Path: " + path)
    if (writers.contains(path)) {
      throw new IllegalArgumentException(path + "writer already exists")
    }

    writers.put(path, writer)
  }

  def getWriter(path: String): CarbonStreamingOutputWriter = {
    writeLog("Inside get Writer")
    writers.get(path)
  }

  def containsWriter(path: String): Boolean = {
    writeLog("Inside contain writer")
    writers.containsKey(path)
  }

  def removeWriter(path: String): Unit = {
    writeLog("Inside remove Writer")
    writers.remove(path)
  }
}