package cz.edee111.dbdumper.dbcopymaker

/**
 * @author Eduard Tomek
 */
class DbConnectionDetails {
  String username
  String password
  String host
  String port
  String dbName

  String getConnectionString() {
    return "jdbc:postgresql://${host}:${port}/${dbName}"
  }
}
