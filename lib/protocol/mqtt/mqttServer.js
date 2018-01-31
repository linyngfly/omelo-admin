let logger = require('pomelo-logger').getLogger('pomelo-admin', 'MqttServer');
let EventEmitter = require('events').EventEmitter;
let MqttCon = require('mqtt-connection');
let Util = require('util');
let net = require('net');

let curId = 1;

let MqttServer = function(opts, cb) {
	EventEmitter.call(this);
	this.inited = false;
	this.closed = true;
};

Util.inherits(MqttServer, EventEmitter);

MqttServer.prototype.listen = function(port) {
	//check status
	if (this.inited) {
		this.cb(new Error('already inited.'));
		return;
	}

	this.inited = true;

	let self = this;

	this.server = new net.Server();
	this.server.listen(port);

	logger.info('[MqttServer] listen on %d', port);

	this.server.on('listening', this.emit.bind(this, 'listening'));

	this.server.on('error', function(err) {
		// logger.error('mqtt server is error: %j', err.stack);
		self.emit('error', err);
	});

	this.server.on('connection', function(stream) {
		let socket = MqttCon(stream);
		socket['id'] = curId++;

		socket.on('connect', function(pkg) {
			socket.connack({
				returnCode: 0
			});
		});

		socket.on('publish', function(pkg) {
			let topic = pkg.topic;
			let msg = pkg.payload.toString();
			msg = JSON.parse(msg);

			// logger.debug('[MqttServer] publish %s %j', topic, msg);
			socket.emit(topic, msg);
		});

		socket.on('pingreq', function() {
			socket.pingresp();
		});

		socket.send = function(topic, msg) {
			socket.publish({
				topic: topic,
				payload: JSON.stringify(msg)
			});
		};

		self.emit('connection', socket);
	});
};

MqttServer.prototype.send = function(topic, msg) {
	this.socket.publish({
		topic: topic,
		payload: msg
	});
}

MqttServer.prototype.close = function() {
	if (this.closed) {
		return;
	}

	this.closed = true;
	this.server.close();
	this.emit('closed');
};

module.exports = MqttServer;