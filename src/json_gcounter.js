"use strict";

class JsonGCounter {
	constructor(replicaName) {
		this.replicaName = replicaName;
		this.data = {[replicaName] : 0};
	}

	inc() {
		this.data[this.replicaName]++;
	}

	state() {
		let sum = 0;
		let keys = Object.keys(this.data);
		for (var i = keys.length - 1; i >= 0; i--) {
			sum += this.data[keys[i]];
		}
		return sum;
	}

	val() {
		return this.data[this.replicaName];
	}

	merge(replica) {
		this.data[replica.key] = replica.val();
	}
}

module.exports = JsonGCounter;