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

package org.apache.spark.sql

import java.io.{BufferedWriter, FileWriter, IOException}
import java.util
import java.util.UUID

import scala.language.implicitConversions
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.CarbonLateDecodeStrategy
import org.apache.spark.sql.execution.command.{BucketFields, CreateTable}
import org.apache.spark.sql.optimizer.CarbonLateDecodeRule
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory}
import org.apache.spark.sql.streaming.CarbonStreamingOutputWriterFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.format.{ColumnSchema, DataType, TableInfo, TableSchema}
import org.apache.parquet.schema.InvalidSchemaException
import org.apache.spark.sql.hive.CarbonMetastore

/**
  * Carbon relation provider compliant to data source api.
  * Creates carbon relations
  */
class CarbonSource extends CreatableRelationProvider with RelationProvider
  with SchemaRelationProvider with DataSourceRegister with FileFormat {

  def writeLog(message: String): Unit = {
    try {
      val bw = new BufferedWriter(new FileWriter("/home/knoldus/test.log", true))
      try {
        bw.append("[CarbonSource] : " + message).append("\n")
        Console.print("[CarbonSource] : " + message)
      } catch {
        case e: IOException =>
          e.printStackTrace()
      } finally if (bw != null) {
        bw.close()
      }
    }
  }

  override def shortName(): String = {
    writeLog("Shortname called")
    "carbondata"
  }

  // will be called if hive supported create table command is provided
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    writeLog("Create Relation [1] : " + parameters)
    CarbonEnv.getInstance(sqlContext.sparkSession)
    // if path is provided we can directly create Hadoop relation. \
    // Otherwise create datasource relation
    parameters.get("tablePath") match {
      case Some(path) => CarbonDatasourceHadoopRelation(sqlContext.sparkSession,
        Array(path),
        parameters,
        None)
      case _ =>
        val options = new CarbonOption(parameters)
        val storePath = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.STORE_LOCATION)
        val tablePath = storePath + "/" + options.dbName + "/" + options.tableName
        CarbonDatasourceHadoopRelation(sqlContext.sparkSession, Array(tablePath), parameters, None)
    }
  }

  // called by any write operation like INSERT INTO DDL or DataFrame.write API
  def createRelation(
                      sqlContext: SQLContext,
                      mode: SaveMode,
                      parameters: Map[String, String],
                      data: DataFrame): BaseRelation = {
    writeLog("Create Relation [2] : " + parameters)
    CarbonEnv.getInstance(sqlContext.sparkSession)
    // User should not specify path since only one store is supported in carbon currently,
    // after we support multi-store, we can remove this limitation
    require(!parameters.contains("path"), "'path' should not be specified, " +
      "the path to store carbon file is the 'storePath' " +
      "specified when creating CarbonContext")

    val options = new CarbonOption(parameters)
    val storePath = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION)
    val tablePath = new Path(storePath + "/" + options.dbName + "/" + options.tableName)
    val isExists = tablePath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
      .exists(tablePath)
    val (doSave, doAppend) = (mode, isExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        sys.error(s"ErrorIfExists mode, path $storePath already exists.")
      case (SaveMode.Overwrite, true) =>
        sqlContext.sparkSession
          .sql(s"DROP TABLE IF EXISTS ${options.dbName}.${options.tableName}")
        (true, false)
      case (SaveMode.Overwrite, false) | (SaveMode.ErrorIfExists, false) =>
        (true, false)
      case (SaveMode.Append, _) =>
        (false, true)
      case (SaveMode.Ignore, exists) =>
        (!exists, false)
    }

    if (doSave) {
      // save data when the save mode is Overwrite.
      new CarbonDataFrameWriter(sqlContext, data).saveAsCarbonFile(parameters)
    } else if (doAppend) {
      new CarbonDataFrameWriter(sqlContext, data).appendToCarbonFile(parameters)
    }

    createRelation(sqlContext, parameters, data.schema)
  }

  // called by DDL operation with a USING clause
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String],
                               dataSchema: StructType): BaseRelation = {
    writeLog("Create Relation [3] : " + parameters)
    CarbonEnv.getInstance(sqlContext.sparkSession)
    addLateDecodeOptimization(sqlContext.sparkSession)
    val dbName: String = parameters.getOrElse("dbName",
      CarbonCommonConstants.DATABASE_DEFAULT_NAME).toLowerCase
    val tableOption: Option[String] = parameters.get("tableName")
    if (tableOption.isEmpty) {
      sys.error("Table creation failed. Table name is not specified")
    }
    val tableName = tableOption.get.toLowerCase()
    if (tableName.contains(" ")) {
      sys.error("Table creation failed. Table name cannot contain blank space")
    }
    val path = if (sqlContext.sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tableName))) {
      getPathForTable(sqlContext.sparkSession, dbName, tableName)
    } else {
      createTableIfNotExists(sqlContext.sparkSession, parameters, dataSchema)
    }

    CarbonDatasourceHadoopRelation(sqlContext.sparkSession, Array(path), parameters,
      Option(dataSchema))
  }

  private def addLateDecodeOptimization(ss: SparkSession): Unit = {
    writeLog("Inside addLateDecodeOptimization")
    if (ss.sessionState.experimentalMethods.extraStrategies.isEmpty) {
      ss.sessionState.experimentalMethods.extraStrategies = Seq(new CarbonLateDecodeStrategy)
      ss.sessionState.experimentalMethods.extraOptimizations = Seq(new CarbonLateDecodeRule)
    }
  }


  private def createTableIfNotExists(sparkSession: SparkSession, parameters: Map[String, String],
                                     dataSchema: StructType): String = {
    writeLog("Inside addLateDecodeOptimization = " + parameters)
    val dbName: String = parameters.getOrElse("dbName",
      CarbonCommonConstants.DATABASE_DEFAULT_NAME).toLowerCase
    val tableName: String = parameters.getOrElse("tableName", "").toLowerCase
    try {
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(Option(dbName), tableName)(sparkSession)
      CarbonEnv.getInstance(sparkSession).carbonMetastore.storePath + s"/$dbName/$tableName"
    } catch {
      case ex: NoSuchTableException =>
        val sqlParser = new CarbonSpark2SqlParser
        val fields = sqlParser.getFields(dataSchema)
        val map = scala.collection.mutable.Map[String, String]()
        parameters.foreach { case (key, value) => map.put(key, value.toLowerCase()) }
        val options = new CarbonOption(parameters)
        val bucketFields = sqlParser.getBucketFields(map, fields, options)
        val cm = sqlParser.prepareTableModel(ifNotExistPresent = false, Option(dbName),
          tableName, fields, Nil, map, bucketFields)
        CreateTable(cm, false).run(sparkSession)
        CarbonEnv.getInstance(sparkSession).carbonMetastore.storePath + s"/$dbName/$tableName"
      case ex: Exception =>
        throw new Exception("do not have dbname and tablename for carbon table", ex)
    }
  }

  /**
    * Returns the path of the table
    *
    * @param sparkSession
    * @param dbName
    * @param tableName
    * @return
    */
  private def getPathForTable(sparkSession: SparkSession, dbName: String,
                              tableName: String): String = {
    writeLog("Inside getPathForTable " + dbName + " ||| " + tableName)
    if (StringUtils.isBlank(tableName)) {
      throw new MalformedCarbonCommandException("The Specified Table Name is Blank")
    }
    if (tableName.contains(" ")) {
      throw new MalformedCarbonCommandException("Table Name Should not have spaces ")
    }
    try {
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(Option(dbName), tableName)(sparkSession)
      CarbonEnv.getInstance(sparkSession).carbonMetastore.storePath + s"/$dbName/$tableName"
    } catch {
      case ex: Exception =>
        throw new Exception(s"Do not have $dbName and $tableName", ex)
    }
  }

  /**
    * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
    * be put here.  For example, user defined output committer can be configured here
    * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
    */
  def prepareWrite(
                    sparkSession: SparkSession,
                    job: Job,
                    options: Map[String, String],
                    dataSchema: StructType): OutputWriterFactory = {
    //    val pathExists: Boolean = options.get("path").isDefined
    val tablePath = options.get("path")
    val path: String = tablePath match {
      case Some(value) => value
      case None => ""
    }
    val meta: CarbonMetastore = new CarbonMetastore(sparkSession.conf, path)
    val schemaPath = path + "/Metadata/schema"
    val schema: TableInfo = meta.readSchemaFile(schemaPath)

    val isValid = validateSchema(schema, dataSchema)
    Console.println("Schema valid ::: " + isValid)
    if(isValid)
    new CarbonStreamingOutputWriterFactory()
    else
      throw new InvalidSchemaException("Exception in schema validation : ")
  }

  def validateSchema(schema: TableInfo, dataSchema: StructType): Boolean = {
    val factTable: TableSchema = schema.getFact_table
    import scala.collection.JavaConversions._
    val columnnSchemaValues = factTable.getTable_columns.toList
    val dataTypeList = List[String]()
    val columnNameList = List[String]()
    val dataTypes = for {cn <- columnnSchemaValues
                         data: List[String] = addToList(dataTypeList, cn.data_type.toString)
    } yield data
    val listOfDatatypes = dataTypes.flatMap(element => element)
    val columnNames = for {cn <- columnnSchemaValues
                           data: List[String] = addToList(columnNameList, cn.column_name)
    } yield data

    val listOfColumnNames = columnNames.flatMap(element => element)
    val pairOfStoredSchema = listOfColumnNames.zip(listOfDatatypes)
    val mapOfStoredTableSchema = pairOfStoredSchema.flatMap(x => addToMap(x, Map[String, String]())).toMap
    Console.println("Map of stored schema ::: " + mapOfStoredTableSchema)

    val StreamedSchema: StructType = dataSchema
    val size = StreamedSchema.size
    val StreamedDataTypeList = List[String]()
    val StreamedDataType = for {i <- 0 until size
                                data: List[String] = addToList(StreamedDataTypeList, StreamedSchema.fields(i).dataType.toString)
    } yield data

    val x = StreamedDataType.flatten.toList
    val parsedDatatypeList = for {list <- x
                                  a = parseStreamingDataType(list)} yield a

    val StreamedColumnNameList = List[String]()
    val StreamedColumnName = for {i <- 0 until size
                                  data: List[String] = addToList(StreamedColumnNameList, StreamedSchema.fields(i).name)
    } yield data

    val StreamedColumns = StreamedColumnName.flatten.toList
    val pairOfStreamedSchema = StreamedColumns.zip(parsedDatatypeList)
    val mapOfStreamedSchema = pairOfStreamedSchema.flatMap(x => addToMap(x, Map[String, String]())).toMap
    Console.println("Map of Streamed schema :::: " + mapOfStreamedSchema)

    //Comparing stored table schema and streamed schema
    val isValid = mapOfStoredTableSchema == mapOfStreamedSchema
    isValid
  }

  def addToList(list1: List[String], element: String): List[String] = {
    List(element) ::: list1
  }

  def addToMap(pair: (String, String), map: Map[String, String]): Map[String, String] = {
    map + (pair._1 -> pair._2)
  }

  def parseStreamingDataType(dataType: String): String = {
    dataType match {
      case "IntegerType" => DataType.INT.toString
      case "StringType" => DataType.STRING.toString
      case "DateType" => DataType.DATE.toString
      case "DoubleType" => DataType.DOUBLE.toString
      case "FloatType" => DataType.DOUBLE.toString
      case "LongType" => DataType.LONG.toString
      case "ShortType" => DataType.SHORT.toString
      case "TimestampType" => DataType.TIMESTAMP.toString
    }
  }


  private def validateTable(path: String): CarbonStreamingOutputWriterFactory = {
    val tableName = path.toString.split("/").toList.reverse.head
    val databaseName = path.toString.split("/").toList.reverse(2)
    val absoluteTableIdentifier: AbsoluteTableIdentifier = new AbsoluteTableIdentifier(path,
      new CarbonTableIdentifier(databaseName, tableName, UUID.randomUUID().toString))
    if (checkIfTableExists(absoluteTableIdentifier)) {
      new CarbonStreamingOutputWriterFactory()
    } else {
      Console.print(s"/$databaseName/$tableName does not exist.")
      throw new NoSuchTableException(databaseName, tableName)
    }
  }

  private def checkIfTableExists(absoluteTableIdentifier: AbsoluteTableIdentifier): Boolean = {
    val carbonTablePath: CarbonTablePath = CarbonStorePath
      .getCarbonTablePath(absoluteTableIdentifier)
    val schemaFilePath: String = carbonTablePath.getSchemaFilePath
    FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.LOCAL) ||
      FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.HDFS) ||
      FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.VIEWFS)
  }

  /**
    * When possible, this method should return the schema of the given `files`.  When the format
    * does not support inference, or no valid files are given should return None.  In these cases
    * Spark will require that user specify the schema manually.
    */
  def inferSchema(
                   sparkSession: SparkSession,
                   options: Map[String, String],
                   files: Seq[FileStatus]): Option[StructType] = {
    writeLog("Infer Schema: " + options)
    Some(new StructType().add("value", StringType))
  }

}
