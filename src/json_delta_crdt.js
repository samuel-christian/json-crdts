"use strict"

const crypto = require("crypto");
const net = require("net");
const JsonSocket = require("json-socket");
const fs = require("fs");

class JsonDeltaCrdt {
	constructor(config_file_name) {
		this.configuration = this.readConfig(config_file_name); // file extension must be supplied
		// CRDT attributes
		this.replicaId = this.configuration.name;
		this.timestamp = [0, this.replicaId];
		this.jsonData = {};
		this.delta = {};
		// TCP connections setup
		this.tcp_port = this.configuration.tcp_port;
		this.sockets = [];
		this.openTcp();
		if (this.configuration.nodes.length > 0) {
			this.clients = [];
			this.nodes = this.configuration.nodes;
			this.initConnection();
		}
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

	removeFromDeltaSet(replicaID) {
		delete this.delta.replicaID;
		// remove from ack array
		for (var key in this.jsonData) {
			var ack_array = this.jsonData[key].ack;
			ack_array.splice(ack_array.indexOf(replicaID), 1);
		}
	}

	endClient(clientName) {
		var foundIndex = -1;
		this.clients.find((client, index) => {
			if (client.name == clientName) {
				foundIndex = index;
			}
		});
		if (foundIndex != -1) {
			this.clients[foundIndex].end();
			this.clients.splice(foundIndex, 1);
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
			// broadcast to neighbour nodes that connect to it
			this.sockets.forEach((socket) => {
				socket.sendMessage({
					type: "delta",
					content: this.delta
				});
			});
			// send to neighbour node if it has one
			if (this.clients != undefined) {
				this.clients.forEach((client) => {
					client.sendMessage({
						type: "delta",
						content: this.delta
					});
				})
			}
		}
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
	initConnection() {
		this.nodes.forEach((node) => {
			var client = new JsonSocket(new net.Socket());
			client.name = node.name;
			client.reconnect = true;
			this.clients.push(client);
		})
		this.applyListener();
	}

	openTcp() {
		var tcp_connection = net.createServer((socket) => {

			var remoteAddress = socket.remoteAddress;
			socket = new JsonSocket(socket);
			// apply listener
			socket.on('message', (data) => {
				if (data.type == "intro") {
					socket.name = data.content;
					console.log("Client " + socket.name + " (" + remoteAddress + ") " + "connected!");
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
			this.sockets.push(socket);

			socket.on('end', () => {
				console.log("Client " + socket.name + " (" + remoteAddress + ") " + "disconnected!");
				this.removeFromDeltaSet(socket.name);
				this.endClient(socket.name);
				this.sockets.splice(this.sockets.indexOf(socket), 1);
			});

			socket.on('error', (err) => {
				console.log("socket " + socket.name + " (" + remoteAddress + ") " + "closed! " + "(" + err.code + ")");
				this.removeFromDeltaSet(socket.name);
				this.sockets.splice(this.sockets.indexOf(socket), 1);
			})
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

	applyListener() {
		this.clients.forEach((client, index) => {
			client.on('connect', () => {
				console.log("connected to " + this.nodes[index].host + ":" + this.nodes[index].port + "!");
				// send server its info
				client.sendMessage({
					type: "intro",
					content: this.replicaId
				});
				client.on('message', (message) => {
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
			client.on('error', (err) => {
				setTimeout(() => {
					console.log("Finding " + this.nodes[index].host + ":" + this.nodes[index].port + "...");
					client.connect(this.nodes[index].port, this.nodes[index].host);
				}, 1000);
			});

			client.on('end', () => {
				if (client.reconnect) {
					console.log("client disconnects, wait 1s to reconnect...");
					setTimeout(() => {
						console.log("(reconnect) Finding " + this.nodes[index].host + ":" + this.nodes[index].port + "...");
						client.connect(this.nodes[index].port, this.nodes[index].host);
					}, 1000);
				} else {
					console.log("No reconnect for you!");
				}
			});
			client.connect(this.nodes[index].port, this.nodes[index].host);
		});
	}

	// CONFIGURATION STUFF
	readConfig(config_file) {
		if (fs.existsSync(config_file)) {
			console.log("Reading configuration file...");
			var raw_config = fs.readFileSync(config_file);
			var config_json = JSON.parse(raw_config);
			return config_json;
		} else {
			console.log("No configuration file is specified, exiting the program...");
			console.log("Please check if the supplied file name must be with .json extension!");
			process.exit(0);
		}
	}

	// FOR EXPERIMENTAL STUFF ONLY
	reconnect() {
		this.clients = [];
		this.initConnection();
	}

	disconnect() {
		this.clients.forEach((client) => {
			client.end(() => {
				client.reconnect = false;
			});
		});
	}
}


module.exports = JsonDeltaCrdt;

