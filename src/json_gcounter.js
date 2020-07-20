var name = "";
var data = {[name]: 0};

function inc() {
	this.data[this.name]++;
}

function state() {
	let sum = 0;
	let keys = Object.keys(data);
	for (var i = keys.length - 1; i >= 0; i--) {
		sum += data[keys[i]];
	}
	return sum;
}

function val() {
	return data[name];
}

function merge(replica) {
	this.data[replica.name] = replica.val();
}

const jsonGCounter = {
	inc,
	state,
	val,
	merge
};

module.exports = jsonGCounter;