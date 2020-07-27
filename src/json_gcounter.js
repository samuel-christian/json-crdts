"use strict";

class GCounter {
	constructor(position) {
		this.name = position;
		this.data = new Array(this.name).fill(0);
	}

	inc() {
		this.data[this.name-1]++;
	}

	val() {
		let sum = 0;
		for (var i = this.data.length - 1; i >= 0; i--) {
			sum += this.data[i];
		}
		return sum;
	}

	state() {
		return this.data[this.name-1];
	}

	merge(replica) {
		this.data[replica.name-1] = replica.state();
	}

	to_json() {
		return {
			name: this.name,
			data: this.data
		};
	}
}

module.exports = GCounter;

// const json_gc = {
// 	name,
// 	data,
// 	inc,
// 	state,
// 	val,
// 	merge,
// 	to_json
// };

// module.exports = json_gc;