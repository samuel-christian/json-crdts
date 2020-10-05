"use strict"

const crypto = require("crypto");

class JsonStateCrdt {
	constructor(id) {
		this.replicaId = id;
		this.timestamp = [0, this.replicaId];
		this.jsonData = {};
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
	}

	delete(key) {
		if (this.has(key)) {
			this.timestamp[0] += 1;
			this.jsonData[key]["ts"] = [this.timestamp[0], this.timestamp[1]];
			this.jsonData[key]["deleted"] = 1;
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
}

module.exports = JsonStateCrdt;

