let logger = require('omelo-logger').getLogger('omelo-admin', __filename);
let utils = require('../util/utils');

let profiler = null;
try {
	profiler = require('v8-profiler');
} catch(e) {
}

let fs = require('fs');
let ProfileProxy = require('../util/profileProxy');

module.exports = function(opts) {
	if (!profiler) {
		return {};
	} else {
		return new Module(opts);
	}
};

if (!profiler) {
	module.exports.moduleError = 1;
}

module.exports.moduleId = 'profiler';

let Module = function(opts) {
	if(opts && opts.isMaster) {
		this.proxy = new ProfileProxy();
	}
};

Module.prototype.monitorHandler = function(agent, msg, cb) {
	let type = msg.type, action = msg.action, uid = msg.uid, result = null;
	if (type === 'CPU') {
		if (action === 'start') {
			profiler.startProfiling();
		} else {
			result = profiler.stopProfiling();
			let res = {};
			res.head = result.getTopDownRoot();
			res.bottomUpHead = result.getBottomUpRoot();
			res.msg = msg;
			agent.notify(module.exports.moduleId, {clientId: msg.clientId, type: type, body: res});
		}
	} else {
		let snapshot = profiler.takeSnapshot();
    let appBase = path.dirname(require.main.filename);
		let name = appBase + '/logs/' + utils.format(new Date()) + '.log';
		let log = fs.createWriteStream(name, {'flags': 'a'});
		let data;
		snapshot.serialize({
			onData: function (chunk, size) {
				chunk = chunk + '';
				data = {
					method:'Profiler.addHeapSnapshotChunk',
					params:{
						uid: uid,
						chunk: chunk
					}
				};
				log.write(chunk);
				agent.notify(module.exports.moduleId, {clientId: msg.clientId, type: type, body: data});
			},
			onEnd: function () {
				agent.notify(module.exports.moduleId, {clientId: msg.clientId, type: type, body: {params: {uid: uid}}});
				profiler.deleteAllSnapshots();
			}
		});
	}
};

Module.prototype.masterHandler = function(agent, msg, cb) {
	if(msg.type === 'CPU') {
		this.proxy.stopCallBack(msg.body, msg.clientId, agent);
	} else {
		this.proxy.takeSnapCallBack(msg.body);
	}
};

Module.prototype.clientHandler = function(agent, msg, cb) {
	if(msg.action === 'list') {
		list(agent, msg, cb);
		return;
	}

	if(typeof msg === 'string') {
		msg = JSON.parse(msg);
	}
	let id = msg.id;
	let command = msg.method.split('.');
	let method = command[1];
	let params = msg.params;
	let clientId = msg.clientId;

	if (!this.proxy[method] || typeof this.proxy[method] !== 'function') {
		return;
	}

	this.proxy[method](id, params, clientId, agent);
};

let list = function(agent, msg, cb) {
	let servers = [];
	let idMap = agent.idMap;

	for(let sid in idMap){
		servers.push(sid);
	}
	cb(null, servers);
};
