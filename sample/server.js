let MasterConsole = require('../lib/consoleService');
let TestModule = require('./module');
let port = 3300;
let host = 'localhost';

let opts = {
	port: port,
	master: true
}

let masterConsole = MasterConsole.createMasterConsole(opts);
let module = TestModule();
masterConsole.register(TestModule.moduleId, module);

masterConsole.start(function() {

})