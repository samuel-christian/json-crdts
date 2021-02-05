"use strict"

const crypto = require("crypto");
const fs = require("fs");
const net = require("net");
const JsonSocket = require("json-socket");

class JsonStateCrdt {
	constructor(config_file_name) {
		this.configuration = this.readConfig(config_file_name);
		// CRDT attributes
		this.replicaId = this.configuration.name;
		this.timestamp = [0, this.replicaId];
		this.jsonData = {};
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
		this.jsonData[key] = { val: value, ts: [this.timestamp[0], this.timestamp[1]] };
		this.broadcast();
	}

	delete(key) {
		if (this.has(key)) {
			this.timestamp[0] += 1;
			this.jsonData[key]["ts"] = [this.timestamp[0], this.timestamp[1]];
			this.jsonData[key]["deleted"] = 1;
			this.broadcast();
			return { [key]: this.jsonData[key] };
		} else {
			return "Key '" + key + "' does not exist.";
		}
	}

	apply(state) {
		let maxClock = this.timestamp[0];
		for (var key in state) {
			maxClock = Math.max(maxClock, state[key].ts[0]);
			if (this.has(key)) {
				let localTS = this.jsonData[key].ts;
				let remoteTS = state[key].ts;
				if ((localTS[0] < remoteTS[0]) || (localTS[0] == remoteTS[0] && this.compareReplica(localTS[1], remoteTS[1]))) {
					// last writer: remote
					this.jsonData[key] = state[key];
				}
			} else {
				this.jsonData[key] = state[key];
			}
		}
		this.timestamp[0] = maxClock + 1;
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
		});
		this.applyListener();
	}

	openTcp() {
		var tcp_connection = net.createServer((socket) => {
			var remoteAddress = socket.remoteAddress;
			socket = new JsonSocket(socket);
			// apply listener
			socket.on('message', (message) => {
				if (message.type == "intro") socket.name = message.from;
				this.apply(message.content);
			});
			this.sockets.push(socket);

			socket.on('end', () => {
				console.log("Client " + socket.name + " (" + remoteAddress + ") " + "disconnected!");
				this.endClient(socket.name);
				this.sockets.splice(this.sockets.indexOf(socket), 1);
			});

			socket.on('error', (err) => {
				console.log("socket " + socket.name + " (" + remoteAddress + ") " + "closed! " + "(" + err.code + ")");
				this.sockets.splice(this.sockets.indexOf(socket), 1);
			})
		});
		tcp_connection.listen(this.tcp_port, () => {
			console.log("TCP connection is opened on port: " + this.tcp_port);
		});
	}

	applyListener() {
		this.clients.forEach((client, index) => {
			client.on('connect', () => {
				// first connect send full state
				console.log("Connected to " + this.nodes[index].host + ":" + this.nodes[index].port);
				client.sendMessage({
					type: "intro",
					from: this.replicaId,
					content: this.jsonData
				});
			});

			client.on('message', (message) => {
				this.apply(message.content);
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
			})
			client.connect(this.nodes[index].port, this.nodes[index].host);
		});
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

	broadcast() {
		// broadcast to neighbours connected to the tcp connection
		this.sockets.forEach((socket) => {
			socket.sendMessage({
				content: this.jsonData
			});
		});
		// sent to neighbours this object connects to
		this.clients.forEach((client) => {
			client.sendMessage({
				content: this.jsonData
			});
		});
	}

	readConfig(filename) {
		if (fs.existsSync(filename)) {
			console.log("Reading configuration file...");
			var raw_config = fs.readFileSync(filename);
			var json_config = JSON.parse(raw_config);
			return json_config
		} else {
			console.log("No configuration file is specified, exiting the program...");
			console.log("Please check if the supplied file name must be with .json extension!");
			process.exit(0);
		}
	}
}

module.exports = JsonStateCrdt;

