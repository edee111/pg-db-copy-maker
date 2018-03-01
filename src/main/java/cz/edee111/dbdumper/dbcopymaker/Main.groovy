package cz.edee111.dbdumper.dbcopymaker

/**
 * @author Eduard Tomek
 */
class Main {

  static void main(String[] args) {
    new DbCopyMaker().run(ConfigCreator.createConfig())
  }

}
