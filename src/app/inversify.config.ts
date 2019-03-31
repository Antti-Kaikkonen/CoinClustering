import EncodingDown from "encoding-down";
import { Container } from "inversify";
import "reflect-metadata";
import rocksDB from 'rocksdb';
import { BinaryDB } from "./services/binary-db";

let cwd = process.cwd();
const config: any = require(cwd+'/config');

const myContainer = new Container({ autoBindInjectable: true, defaultScope: "Singleton" });
myContainer.bind<string>("string").toConstantValue(config.protocol).whenTargetNamed("protocol");
myContainer.bind<string>("string").toConstantValue(config.user).whenTargetNamed("user");
myContainer.bind<string>("string").toConstantValue(config.pass).whenTargetNamed("pass");
myContainer.bind<string>("string").toConstantValue(config.host).whenTargetNamed("host");
myContainer.bind<string>("number").toConstantValue(config.port).whenTargetNamed("port");
myContainer.bind<number>("number").toConstantValue(config.listen_port).whenTargetNamed("listen_port");
myContainer.bind<number>("number").toConstantValue(config.pubkeyhash).whenTargetNamed("pubkeyhash");
myContainer.bind<number>("number").toConstantValue(config.scripthash).whenTargetNamed("scripthash");
myContainer.bind<string>("string").toConstantValue(config.segwitprefix).whenTargetNamed("segwitprefix");
myContainer.bind<number>("number").toConstantValue(config.dbcache).whenTargetNamed("dbcache");

let rocksdb = rocksDB(cwd+'/rocksdb');
let db: BinaryDB = new BinaryDB(EncodingDown<Buffer, Buffer>(rocksdb, {keyEncoding: 'binary', valueEncoding: 'binary'}), {
  writeBufferSize: 16 * 1024 * 1024,
  cacheSize: config.dbcache * 1024 * 1024,
  compression: true
});
myContainer.bind<BinaryDB>(BinaryDB).toConstantValue(db);

export { myContainer };

