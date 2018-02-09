/*!
 * Pomelo -- consoleModule nodeInfo processInfo
 * Copyright(c) 2012 fantasyni <fantasyni@163.com>
 * MIT Licensed
 */
let monitor = require('omelo-monitor');
let logger = require('omelo-logger').getLogger('omelo-admin', __filename);

let DEFAULT_INTERVAL = 5 * 60;		// in second
let DEFAULT_DELAY = 10;						// in second

module.exports = function(opts) {
	return new Module(opts);
};

module.exports.moduleId = 'nodeInfo';

let Module = function(opts) {
	opts = opts || {};
	this.type = opts.type || 'pull';
	this.interval = opts.interval || DEFAULT_INTERVAL;
	this.delay = opts.delay || DEFAULT_DELAY;
};

Module.prototype.monitorHandler = function(agent, msg, cb) {
	let serverId = agent.id;
	let pid = process.pid;
	let params = {
		serverId: serverId,
		pid: pid
	};
	monitor.psmonitor.getPsInfo(params, function (err, data) {
		agent.notify(module.exports.moduleId, {serverId: agent.id, body: data});
	});

};

Module.prototype.masterHandler = function(agent, msg, cb) {
	if(!msg) {
		agent.notifyAll(module.exports.moduleId);
		return;
	}

	let body=msg.body;
	let data = agent.get(module.exports.moduleId);
	if(!data) {
		data = {};
		agent.set(module.exports.moduleId, data);
	}

	data[msg.serverId] = body;
};

Module.prototype.clientHandler = function(agent, msg, cb) {
	cb(null, agent.get(module.exports.moduleId) || {});
};
