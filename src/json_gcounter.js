var name = "";
var data = {};

function getName() {
	return name;
}

function getData() {
	return data;
}

function inc() {
	if (Object.keys(data).length == 0) {
		return "No name set for replica. You can call setName(n) method";
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

function setName(n) {
	name = n;
	data = {[name]: 0};
}

function to_json() {
	return {
		name: name,
		data: data
	};
}

const json_gc = {
	inc,
	state,
	val,
	merge,
	setName,
	getName,
	getData,
	to_json
};

module.exports = json_gc;