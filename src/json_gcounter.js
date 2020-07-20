var name = "";
var data = {};

function inc() {
	if (Object.keys(data).length == 0) {
		return "No name set for replica. You can call set(name) method";
	} else {
		data[name]++;
	}
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
	data[replica.name] = replica.val();
}

function set(n) {
	name = n;
	data = {[name]: 0};
}

const jsonGCounter = {
	name,
	data,
	inc,
	state,
	val,
	merge
};

module.exports = jsonGCounter;