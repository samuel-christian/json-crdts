"use strict"

const crypto = require("crypto");
const net = require("net");
const JsonSocket = require("json-socket");

class JsonDeltaCrdt {
	constructor(id, tcp_open, host_to, connect_to_port) {
		// CRDT attributes
		this.replicaId = id;
		this.timestamp = [0, this.replicaId];
		this.jsonData = {};
		this.delta = {};
		// TCP connections setup
		this.port = connect_to_port;
		this.host = host_to;
		this.tcp_port = tcp_open;
		this.sockets = [];
		this.openTcp();
		this.client = new JsonSocket(new net.Socket());
		this.client.connect(connect_to_port, host_to);
		this.applyListener();
	}

	addToDeltaSet(replicaID) {
		if (this.delta[replicaID] == undefined) {
			this.delta[replicaID] = {};
			this.computeDelta();
		} else {
			let msg = "Already registered as neighbours!";
			return msg;
		}
	}

	get(key) {
		if (this.jsonData[key] != undefined) {
			if (this.jsonData[key]["deleted"] == undefined) {
				return this.jsonData[key].val;
			}
		}
	}

	has(key) {
		return this.jsonData[key] != undefined;
	}

	state() {
		// print only values of the replica nicely (w/o timestamp, delta, and tombstones)
		let result = {};
		for (var key in this.jsonData) {
			if (this.jsonData[key]["deleted"] == undefined) {
				result[key] = this.jsonData[key].val;
			}
		}
		return result;
	}

	add(key, value) {
		this.timestamp[0] += 1;
		this.jsonData[key] = { val: value, ts: [this.timestamp[0], this.timestamp[1]], ack: [this.replicaId] };
		this.computeDelta();
		return this.delta;
	}

	delete(key) {
		if (this.has(key)) {
			this.timestamp[0] += 1;
			this.jsonData[key]["ts"] = [this.timestamp[0], this.timestamp[1]];
			this.jsonData[key]["ack"] = [this.replicaId];
			this.jsonData[key]["deleted"] = 1;
			this.computeDelta();
			return { [key]: this.jsonData[key] };
		} else {
			return "Key '" + key + "' does not exist.";
		}
	}

	computeDelta() {
		for (var key in this.jsonData) {
			var ackArr = this.jsonData[key].ack;
			for (var deltaKey in this.delta) {
				if (!ackArr.includes(deltaKey)) {
					this.delta[deltaKey][key] = this.jsonData[key];
				}
			}
		}
		if (!this.isDeltaEmpty()) {
			this.client.sendMessage({
				type: "delta",
				content: this.delta
			});
		}
		return this.delta;
	}

	receive(ack) {
		if (ack != undefined) {
			// for delta
			var remoteID = Object.keys(ack)[0];
			for (var i = ack[remoteID].val.length - 1; i >= 0; i--) {
				var key = ack[remoteID].val[i];
				if (!this.jsonData[key].ack.includes(remoteID)) {
					this.jsonData[key].ack.push(remoteID);
				}
			}
			// update local clock upon receiving ack message
			this.timestamp[0] = Math.max(this.timestamp[0], ack[remoteID]["currentTS"]);
			// clear delta
			this.delta[remoteID] = {};
		}
	}

	isDeltaEmpty(delta) {
		var res = true;
		for (var key in this.delta) {
			if (Object.keys(this.delta[key]).length != 0) {
				res = false;
				break;
			}
		}
		return res;
	}

	// apply received delta
	apply(delta) {
		// deep copy only for testing purpose
		var deltaForReplica = delta[this.replicaId];
		if (Object.keys(deltaForReplica).length === 0) {
			// empty delta
			// do nothing
			return;
		} else {
			let ack = { [this.replicaId]: { val: [], currentTS: 0 } };
			let maxClock = this.timestamp[0];
			for (var key in deltaForReplica) {
				maxClock = Math.max(maxClock, deltaForReplica[key].ts[0]);
				if (this.has(key)) {
					let localTS = this.jsonData[key].ts;
					let remoteTS = deltaForReplica[key].ts;
					if ((localTS[0] < remoteTS[0]) || (localTS[0] == remoteTS[0] && this.compareReplica(localTS[1], remoteTS[1]))) {
						// last writer: remote
						this.jsonData[key] = deltaForReplica[key];
						// this.jsonData[key].ts = deltaForReplica[key].ts;
						// this.jsonData[key].ack = deltaForReplica[key].ack;
					}
				} else {
					this.jsonData[key] = deltaForReplica[key];
				}
				// acknowledge
				if (!this.jsonData[key].ack.includes(this.replicaId)) {
					this.jsonData[key].ack.push(this.replicaId);
				}
				ack[this.replicaId].val.push(key);
			}
			this.timestamp[0] = maxClock + 1;
			ack[this.replicaId].currentTS = this.timestamp[0];
			this.computeDelta();
			return ack;
		}
	}

	createHash(str) {
		return crypto.createHash("sha256").update(str).digest("hex");
	}

	parseHex(hexString) {
		return parseInt(hexString, 16);
	}

	compareReplica(a, b) {
		let hashA = this.createHash(a);
		let hashB = this.createHash(b);
		return this.parseHex(hashA) < this.parseHex(hashB);
	}

	// CONNECTION STUFF
	openTcp() {
		var tcp_connection = net.createServer((socket) => {
			var socket_name = socket.remoteAddress + ":" + socket.remotePort;
			console.log("Client " + socket_name + " connected!");

			socket = new JsonSocket(socket);
			socket.name = socket_name;
			this.sockets.push(socket);
			// apply listener
			socket.on('message', (data) => {
				if (data.type == "intro") {
					this.addToDeltaSet(data.content);
					if (this.isDeltaEmpty()) {
						// transfer full state
						socket.sendMessage({
							type: "intro",
							content: this.jsonData,
							replicaId: this.replicaId
						});
					} else {
						socket.sendMessage({
							type: "delta",
							content: this.delta
						});
					}
				} else if (data.type == "delta") {
					var ack_message = this.apply(data.content);
					this.broadcast(socket, data, ack_message);
				} else {
					this.receive(data.content);
				}
			});

			socket.on('end', () => {
				console.log("Client " + socket_name + " disconnected!");
				this.sockets.splice(this.sockets.indexOf(socket), 1);
			});
		});
		tcp_connection.listen(this.tcp_port, () => {
			console.log("TCP connection is opened on port: " + this.tcp_port);
		});
	}

	broadcast(from, data, ack_message) {
		if (sockets.length === 0) return;
		sockets.forEach((socket) => {
			if (socket.name === from.name) {
				from.sendMessage({
					type: "ack",
					content: ack_message
				});
			} else {
				socket.sendMessage({
					type: "delta",
					content: this.delta
				});
			}
		});
	}

	reconnect() {
		this.client = new JsonSocket(new net.Socket());
		this.client.connect(this.port, this.host);
		this.applyListener();
	}

	disconnect() {
		this.client.end();
	}

	applyListener() {
		this.client.on('connect', () => {
			console.log("connected!");
			// this.addToDeltaSet("http://"+this.host+":"+this.port);
			// send server its info
			this.client.sendMessage({
				type: "intro",
				content: this.replicaId
			});
			this.client.on('message', (message) => {
				if (message.type == "ack") {
					this.receive(message.content);
				} else if (message.type == "delta") {
					var ack_message = this.apply(message.content);
					// send ack message back to server
					this.client.sendMessage({
						type: "ack",
						content: ack_message
					});
				} else if (message.type == "intro") {
					this.addToDeltaSet(message.replicaId);
					// apply full state
					this.jsonData = message.content;
					this.computeDelta();
				}
			});
		});
		this.client.on('error', (err) => {
			setTimeout(() => {
				console.log("Trying to reconnect...");
				this.client.connect(this.port, this.host);
			}, 1000);
			// (error.errno == -61) ? console.log("The server is off! Turn it on first!") : console.log(error);
		});
	}
}

module.exports = JsonDeltaCrdt;

