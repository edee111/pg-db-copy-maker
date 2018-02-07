package cz.edee111.dbdumper.dbcopymaker

/**
 * @author Eduard Tomek
 */
class ConfigCreator {

  static Config createConfig() {

    return new Config(
        dbSrc: new DbConnectionDetails(
            username: "",
            password: "",
            host: "localhost",
            port: "25432",
            dbName: ""
        ),
        dbDst: new DbConnectionDetails(
            username: "",
            password: "masterkey",
            host: "localhost",
            port: "5432",
            dbName: ""
        ),
        makePause: true,
        makePauseVariabilitySeconds: 17,
        makePauseFixedSeconds: 13,
        schemaPattern: '',
        tableSizeLimit: 200000,
        startFromTableName: null,
        processSingleTableName: null
    )
  }

}
