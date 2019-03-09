import { Container } from "inversify";
import "reflect-metadata";

let cwd = process.cwd();
const config: any = require(cwd+'/config');

const myContainer = new Container({ autoBindInjectable: true });
myContainer.bind<string>("string").toConstantValue(config.user).whenTargetNamed("user");
myContainer.bind<string>("string").toConstantValue(config.pass).whenTargetNamed("pass");
myContainer.bind<string>("string").toConstantValue(config.host).whenTargetNamed("host");
myContainer.bind<string>("string").toConstantValue(config.port).whenTargetNamed("port");
myContainer.bind<number>("number").toConstantValue(config.listen_port).whenTargetNamed("listen_port");
myContainer.bind<number>("number").toConstantValue(config.pubkeyhash).whenTargetNamed("pubkeyhash");
myContainer.bind<number>("number").toConstantValue(config.scripthash).whenTargetNamed("scripthash");
myContainer.bind<string>("string").toConstantValue(config.segwitprefix).whenTargetNamed("segwitprefix");
myContainer.bind<number>("number").toConstantValue(config.dbcache).whenTargetNamed("dbcache");

export { myContainer };
