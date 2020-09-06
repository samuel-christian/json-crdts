"use strict"

const crypto = require("crypto");

class JsonDeltaCrdt {
	constructor(id) {
		this.replicaId = id;
		this.timestamp = [0, this.replicaId];
		this.jsonData = {};
		this.delta = {};
	}

	connect(replicaID) {
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
		return this.computeDelta();
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
		return this.parseHex(a) < this.parseHex(b);
	}
}

module.exports = JsonDeltaCrdt;

