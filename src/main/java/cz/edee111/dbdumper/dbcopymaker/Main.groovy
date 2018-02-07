package cz.edee111.dbdumper.dbcopymaker

import cz.edee111.dbdumper.DbCopyMaker

/**
 * @author Eduard Tomek
 */
class Main {

  static void main(String[] args) {
    new DbCopyMaker().run(ConfigCreator.createConfig())
  }

}
