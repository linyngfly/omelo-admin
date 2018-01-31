let MonitorConsole = require('../lib/consoleService');
let TestModule = require('./module');
let port = 3300;
// let host = '192.168.131.1';
let host = 'localhost';

let opts = {
	id: 'test-server-1',
	type: 'test',
	host: host,
	port: port,
	info: {
		id: 'test-server-1',
		host: host,
		port: 4300
	}
}

let monitorConsole = MonitorConsole.createMonitorConsole(opts);
let module = TestModule();
monitorConsole.register(TestModule.moduleId, module);

monitorConsole.start(function() {

})