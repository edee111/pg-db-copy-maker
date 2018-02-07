package cz.edee111.dbdumper.dbcopymaker

/**
 * @author Eduard Tomek
 */
class Config {

  DbConnectionDetails dbSrc
  DbConnectionDetails dbDst

  boolean makePause
  int makePauseVariabilitySeconds
  int makePauseFixedSeconds

  String schemaPattern
  long tableSizeLimit
  String startFromTableName
  String processSingleTableName

}
