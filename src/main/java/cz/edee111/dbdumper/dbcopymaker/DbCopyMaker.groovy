package cz.edee111.dbdumper

import cz.edee111.dbdumper.dbcopymaker.DbConnectionDetails
import groovy.sql.GroovyResultSet
import groovy.sql.GroovyResultSetExtension
import groovy.sql.Sql

import java.sql.ResultSet

class DbDumper {

  @SuppressWarnings("GroovyUnusedDeclaration")
  private static final DbConnectionDetails dbSrc = new DbConnectionDetails(
      username: "",
      password: "",
      host: "localhost",
      port: "25432",
      dbName: ""
  )

  @SuppressWarnings("GroovyUnusedDeclaration")
  private static final DbConnectionDetails dbDst = new DbConnectionDetails(
      username: DB_USER,
      password: "masterkey",
      host: "localhost",
      port: "5432",
      dbName: ""
  )

  private static String DB_USER = ""
  private static boolean MAKE_PAUSE = true
  private static int MAKE_PAUSE_SECONDS_VARIABLITY = 17
  private static int MAKE_PAUSE_SECONDS_FIXED = 9

  private static long TABLE_SIZE_LIMIT = 12000000
  private static String START_FROM_TABLE_NAME = null //"aggregation_product"
  private static String PROCESS_SINGLE_TABLE_NAME = null;

  Sql sqlSrc
  Sql sqlDst

  void run () {
    try {
      prepareDbConnections(dbSrc, dbDst)
      copyData()
    }
    finally {
      closeDbConnections()
    }
  }

  void copyData() {
    sqlDst.withTransaction {
      def schemas = new GroovyResultSetExtension(sqlSrc.connection.metaData.getSchemas(null, ''))
      schemas.eachRow { schema ->
        //noinspection GroovyAssignabilityCheck
        copySchema(schema.table_schem)
      }
    }
  }

  void copySchema(String schema) {
    if (PROCESS_SINGLE_TABLE_NAME != null) {
      copyTable(schema, PROCESS_SINGLE_TABLE_NAME);
    }
    else {
      def tables = new GroovyResultSetExtension(sqlSrc.connection.metaData.getTables(null, schema, '%', 'TABLE'))
      tables.eachRow { table ->
        //noinspection GroovyAssignabilityCheck
        copyTable(table.table_schem, table.table_name)
      }
    }

  }

  private static boolean doReallyCopy = false

  void copyTable(String schema, String table) {
    if (!START_FROM_TABLE_NAME || START_FROM_TABLE_NAME == table) {
      doReallyCopy = true
    }
    if (!doReallyCopy) {
      return
    }
    println("${schema}.${table} - start")
    println("${schema}.${table} - drop Fk constrains")
    dropFkConstraints(schema, table)
    println("${schema}.${table} - truncate")
    truncateTable(schema, table)
    doCopyTable(schema, table)
    println("${schema}.${table} - move sequences")
    moveSequences(schema, table)
    println("${schema}.${table} - end")
    makePause()
  }

  void dropFkConstraints(String schema, String table) {
    def sql = "SELECT tc.constraint_name as constraint_name " +
        "FROM information_schema.table_constraints AS tc " +
        "WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='${table}';"

    sqlDst.eachRow(sql as String) { GroovyResultSet row ->
      dropFkConstraint(schema, table, row['constraint_name'] as String)
    }
  }

  void dropFkConstraint(String schema, String table, String constraint) {
    def sql = "ALTER TABLE ${schema}.${table} DROP CONSTRAINT ${constraint};"
    sqlDst.execute(sql as String)
  }

  void truncateTable(String schema, String table) {
    def sql = "DELETE FROM ${schema}.${table}"
    sqlDst.execute(sql as String)
  }

  void doCopyTable(String schema, String table) {
    def columns = new GroovyResultSetExtension(sqlSrc.connection.metaData.getColumns(null, schema, table, null))
    def columnNames = []
    columns.eachRow {
      columnNames << it.COLUMN_NAME
    }

    def rowCount = sqlSrc.firstRow("SELECT COUNT(*) as ROW_COUNT FROM " + schema + '.' + table).get('ROW_COUNT') as Integer
    if (columnNames.contains("id")) {
      println "Original row count in table ${schema}.${table}: ${rowCount}."
      if (rowCount > TABLE_SIZE_LIMIT) {
        rowCount = TABLE_SIZE_LIMIT
        println "Setting row count for table ${schema}.${table} to ${rowCount}."
      }
    }


    int i = 0
    List<String> values = []
    String columnNamesStr = prepareColumnNamesStr(columnNames)

    sqlSrc.eachRow("SELECT * FROM ${schema}.${table} ${ columnNames.contains('id') ? "ORDER BY id DESC LIMIT ${TABLE_SIZE_LIMIT}" : ""} " as String) { GroovyResultSet row ->
      values << prepareValues(row, columnNames)

      if (i % 1000 == 0) {
        copyRows(schema, table, values, columnNamesStr)

        if (i % 5000 == 0) {
          println("${schema}.${table} - ${(i * 100) / rowCount}%")

          if (i % 200000 == 0) {
            sqlDst.commit() // asi je zbytecny
          }
        }
      }

      i++
    }
    copyRows(schema, table, values, columnNamesStr)
  }

  static String prepareColumnNamesStr(List<String> columnNames) {
    return columnNames.collect {
      "${it}"
    }
    .join(', ')
  }

  static String prepareValues(row, List<String> columnNames) {
    return '(' + columnNames.collect {
      row[it] != null ? "'${safeStringValue(row[it] as String)}'" : null
    }
    .join(', ') + ')'
  }

  void copyRows(String schema, String table, List<String> values, String columnNamesStr) {
    if (values.size() > 0) {
      def sqlInsert = "INSERT INTO ${schema}.${table} (${columnNamesStr}) VALUES " + values.join(', ')
      sqlDst.execute(sqlInsert as String)
      values.clear()
    }
  }

  void moveSequences(String schema, String table) {
    def sql = "SELECT 'SELECT SETVAL(' ||\n" +
        "       quote_literal(quote_ident(PGT.schemaname) || '.' || quote_ident(S.relname)) ||\n" +
        "       ', COALESCE(MAX(' ||quote_ident(C.attname)|| '), 1) ) FROM ' ||\n" +
        "       quote_ident(PGT.schemaname)|| '.'||quote_ident(T.relname)|| ';' as sql_seq_move_string\n" +
        "FROM pg_class AS S,\n" +
        "     pg_depend AS D,\n" +
        "     pg_class AS T,\n" +
        "     pg_attribute AS C,\n" +
        "     pg_tables AS PGT\n" +
        "WHERE S.relkind = 'S'\n" +
        "    AND S.oid = D.objid\n" +
        "    AND D.refobjid = T.oid\n" +
        "    AND D.refobjid = C.attrelid\n" +
        "    AND D.refobjsubid = C.attnum\n" +
        "    AND T.relname = PGT.tablename\n" +
        "    AND PGT.schemaname = '$schema'\n" +
        "    AND T.relname = '$table'\n" +
        "ORDER BY S.relname;"

    sqlDst.eachRow(sql as String) { GroovyResultSet row ->
      //noinspection GrUnresolvedAccess
      sqlDst.execute(row.sql_seq_move_string as String)
    }
  }

  static void makePause() {
    if (!MAKE_PAUSE) {
      return
    }
    Random random = new Random()
    def variableRandomPart = random.nextInt(MAKE_PAUSE_SECONDS_VARIABLITY)
    def sleepTime = (MAKE_PAUSE_SECONDS_FIXED + variableRandomPart) * 1000 as Long
    println 'Sleeping for ' + sleepTime + ' ms'
    sleep(sleepTime)
  }

  static String safeStringValue(String value) {
    return value.replaceAll("'", "''")
  }

  void prepareDbConnections(DbConnectionDetails src, DbConnectionDetails dst) {
    sqlSrc = Sql.newInstance(src.connectionString, src.username, src.password)
    sqlSrc.setResultSetConcurrency(ResultSet.CONCUR_UPDATABLE)

    sqlDst = Sql.newInstance(dst.connectionString, dst.username, dst.password)
    sqlDst.setResultSetConcurrency(ResultSet.CONCUR_UPDATABLE)
  }

  void closeDbConnections() {
    if (sqlSrc) {
      sqlSrc.close()
    }
    if (sqlDst) {
      sqlDst.close()
    }
  }

  static void main(String[] args) {
    new DbDumper().run()
  }
}