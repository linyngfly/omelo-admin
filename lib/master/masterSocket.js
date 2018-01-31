let logger = require('pomelo-logger').getLogger('pomelo-admin', 'MasterSocket');
let Constants = require('../util/constants');
let protocol = require('../util/protocol');

let MasterSocket = function() {
	this.id = null;
	this.type = null;
	this.info = null;
	this.agent = null;
	this.socket = null;
	this.username = null;
	this.registered = false;
}

MasterSocket.prototype.onRegister = function(msg) {
	if (!msg || !msg.type) {
		return;
	}

	let self = this;
	let serverId = msg.id;
	let serverType = msg.type;
	let socket = this.socket;

	if (serverType == Constants.TYPE_CLIENT) {
		// client connection not join the map
		this.id = serverId;
		this.type = serverType;
		this.info = 'client';
		this.agent.doAuthUser(msg, socket, function(err) {
			if (err) {
				return socket.disconnect();
			}

			self.username = msg.username;
			self.registered = true;
		});
		return;
	} // end of if(serverType === 'client')

	if (serverType == Constants.TYPE_MONITOR) {
		if (!serverId) {
			return;
		}

		// if is a normal server
		this.id = serverId;
		this.type = msg.serverType;
		this.info = msg.info;
		this.agent.doAuthServer(msg, socket, function(err) {
			if (err) {
				return socket.disconnect();
			}

			self.registered = true;
		});

		this.repushQosMessage(serverId);
		return;
	} // end of if(serverType === 'monitor') 

	this.agent.doSend(socket, 'register', {
		code: protocol.PRO_FAIL,
		msg: 'unknown auth master type'
	});

	socket.disconnect();
}

MasterSocket.prototype.onMonitor = function(msg) {
	let socket = this.socket;
	if (!this.registered) {
		// not register yet, ignore any message
		// kick connections
		socket.disconnect();
		return;
	}

	let self = this;
	let type = this.type;
	if (type === Constants.TYPE_CLIENT) {
		logger.error('invalid message from monitor, but current connect type is client.');
		return;
	}

	msg = protocol.parse(msg);
	let respId = msg.respId;
	if (respId) {
		// a response from monitor
		let cb = self.agent.callbacks[respId];
		if (!cb) {
			logger.warn('unknown resp id:' + respId);
			return;
		}

		let id = this.id;
		if (self.agent.msgMap[id]) {
			delete self.agent.msgMap[id][respId];
		}
		delete self.agent.callbacks[respId];
		return cb(msg.error, msg.body);
	}

	// a request or a notify from monitor
	self.agent.consoleService.execute(msg.moduleId, 'masterHandler', msg.body, function(err, res) {
		if (protocol.isRequest(msg)) {
			let resp = protocol.composeResponse(msg, err, res);
			if (resp) {
				self.agent.doSend(socket, 'monitor', resp);
			}
		} else {
			//notify should not have a callback
			logger.warn('notify should not have a callback.');
		}
	});
}

MasterSocket.prototype.onClient = function(msg) {
	let socket = this.socket;
	if (!this.registered) {
		// not register yet, ignore any message
		// kick connections
		return socket.disconnect();
	}

	let type = this.type;
	if (type !== Constants.TYPE_CLIENT) {
		logger.error('invalid message to client, but current connect type is ' + type);
		return;
	}

	msg = protocol.parse(msg);

	let msgCommand = msg.command;
	let msgModuleId = msg.moduleId;
	let msgBody = msg.body;

	let self = this;

	if (msgCommand) {
		// a command from client
		self.agent.consoleService.command(msgCommand, msgModuleId, msgBody, function(err, res) {
			if (protocol.isRequest(msg)) {
				let resp = protocol.composeResponse(msg, err, res);
				if (resp) {
					self.agent.doSend(socket, 'client', resp);
				}
			} else {
				//notify should not have a callback
				logger.warn('notify should not have a callback.');
			}
		});
	} else {
		// a request or a notify from client
		// and client should not have any response to master for master would not request anything from client
		self.agent.consoleService.execute(msgModuleId, 'clientHandler', msgBody, function(err, res) {
			if (protocol.isRequest(msg)) {
				let resp = protocol.composeResponse(msg, err, res);
				if (resp) {
					self.agent.doSend(socket, 'client', resp);
				}
			} else {
				//notify should not have a callback
				logger.warn('notify should not have a callback.');
			}
		});
	}
}

MasterSocket.prototype.onReconnect = function(msg, pid) {
	// reconnect a new connection
	if (!msg || !msg.type) {
		return;
	}

	let serverId = msg.id;
	if (!serverId) {
		return;
	}

	let socket = this.socket;

	// if is a normal server
	if (this.agent.idMap[serverId]) {
		// id has been registered
		this.agent.doSend(socket, 'reconnect_ok', {
			code: protocol.PRO_FAIL,
			msg: 'id has been registered. id:' + serverId
		});
		return;
	}

	let msgServerType = msg.serverType;
	let record = this.agent.addConnection(this.agent, serverId, msgServerType, msg.pid, msg.info, socket);

	this.id = serverId;
	this.type = msgServerType;
	this.registered = true;
	msg.info.pid = pid;
	this.info = msg.info;
	this.agent.doSend(socket, 'reconnect_ok', {
		code: protocol.PRO_OK,
		msg: 'ok'
	});

	this.agent.emit('reconnect', msg.info);

	this.repushQosMessage(serverId);
}

MasterSocket.prototype.onDisconnect = function() {
	let socket = this.socket;
	if (socket) {
		delete this.agent.sockets[socket.id];
	}

	let registered = this.registered;
	if (!registered) {
		return;
	}

	let id = this.id;
	let type = this.type;
	let info = this.info;
	let username = this.username;

	logger.debug('disconnect %s %s %j', id, type, info);
	if (registered) {
		this.agent.removeConnection(this.agent, id, type, info);
		this.agent.emit('disconnect', id, type, info);
	}

	if (type === Constants.TYPE_CLIENT && registered) {
		logger.info('client user ' + username + ' exit');
	}

	this.registered = false;
	this.id = null;
	this.type = null;
}

MasterSocket.prototype.repushQosMessage = function(serverId) {
	let socket = this.socket;
	// repush qos message
	let qosMsgs = this.agent.msgMap[serverId];

	if (!qosMsgs) {
		return;
	}

	logger.debug('repush qos message %j', qosMsgs);

	for (let reqId in qosMsgs) {
		let qosMsg = qosMsgs[reqId];
		let moduleId = qosMsg['moduleId'];
		let tmsg = qosMsg['msg'];

		this.agent.sendToMonitor(socket, reqId, moduleId, tmsg);
	}
}

MasterSocket.prototype.onError = function(err) {
	// logger.error('server %s error %s', this.id, err.stack);
	// this.onDisconnect();
}

module.exports = MasterSocket;