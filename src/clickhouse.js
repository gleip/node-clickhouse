var http = require('http');
var https = require('https');
var url = require('url');
var qs = require('querystring');
var util = require('util');

// var debug = require ('debug')('clickhouse');

require('./legacy-support');

var RecordStream = require('./streams').RecordStream;
var JSONStream = require('./streams').JSONStream;

var parseError = require('./parse-error');

function httpResponseHandler(stream, reqParams, reqData, cb, response) {
	var str;
	var error;

	if (response.statusCode === 200) {
		str = Buffer.alloc ? Buffer.alloc(0) : new Buffer(0);
	} else {
		error = Buffer.alloc ? Buffer.alloc(0) : new Buffer(0);
	}

	function errorHandler(e) {
		var err = parseError(e);

		// user should define callback or add event listener for the error event
		if (!cb || (cb && stream.listeners('error').length))
			stream.emit('error', err);
		return cb && cb(err);
	}

	// In case of error, we're just throw away data
	response.on('error', errorHandler);

	// TODO: use streaming interface
	// from https://github.com/jimhigson/oboe.js
	// or https://www.npmjs.com/package/stream-json or
	// or https://github.com/creationix/jsonparse

	// or implement it youself
	var jsonParser = new JSONStream(stream);

	var symbolsTransferred = 0;

	//another chunk of data has been received, so append it to `str`
	response.on('data', function (chunk) {

		symbolsTransferred += chunk.length;

		// JSON response
		if (
			response.headers['content-type'] &&
			response.headers['content-type'].indexOf('application/json') === 0 &&
			!reqData.syncParser &&
			chunk.lastIndexOf("\n") !== -1 &&
			str
		) {

			// store in buffer anything after
			var newLinePos = chunk.lastIndexOf("\n");

			var remains = chunk.slice(newLinePos + 1);

			Buffer.concat([str, chunk.slice(0, newLinePos)])
				.toString('utf8')
				.split("\n")
				.forEach(jsonParser);

			jsonParser.rows.forEach(function (row) {
				// write to readable stream
				stream.push(row);
			});

			jsonParser.rows = [];

			str = remains;

			// plaintext response
		} else if (str) {
			str = Buffer.concat([str, chunk]);
		} else {
			error = Buffer.concat([error, chunk]);
		}
	});

	//the whole response has been received, so we just print it out here
	response.on('end', function () {

		// debug (response.headers);

		if (error) {
			return errorHandler(error);
		}

		var data;

		var contentType = response.headers['content-type'];

		if (response.statusCode === 200 && (
				!contentType ||
				contentType.indexOf('text/plain') === 0 ||
				contentType.indexOf('text/html') === 0 // WTF: xenial - no content-type, precise - text/html
			)) {
			// probably this is a ping response or any other successful response with *empty* body
			stream.push(null);
			cb && cb(null, str.toString('utf8'));
			return;
		}

		var supplemental = {};

		// we already pushed all the data
		if (jsonParser.columns.length) {
			try {
				supplemental = JSON.parse(jsonParser.supplementalString + str.toString('utf8'));
			} catch (e) {
				// TODO
			}
			stream.supplemental = supplemental;

			// end stream
			stream.push(null);

			cb && cb(null, Object.assign({}, supplemental, {
				meta: jsonParser.columns,
				transferred: symbolsTransferred
			}));

			return;
		}

		// one shot data parsing, should be much faster for smaller datasets
		try {
			data = JSON.parse(str.toString('utf8'));

			data.transferred = symbolsTransferred;

			if (data.meta) {
				stream.emit('metadata', data.meta);
			}

			if (data.data) {
				// no highWatermark support
				data.data.forEach(function (row) {
					stream.push(row);
				});

				stream.push(null);
			}
		} catch (e) {
			if (!reqData.format || !reqData.format.match(/^(JSON|JSONCompact)$/)) {
				data = str.toString('utf8');
			} else {
				return errorHandler(e);
			}
		}

		cb && cb(null, data);
	});

}

function httpRequest(reqParams, reqData, cb) {

	if (reqParams.query) {
		reqParams.path = (reqParams.pathname || reqParams.path) + '?' + qs.stringify(reqParams.query);
	}

	var stream = new RecordStream({
		format: reqData.format
	});
	var requestInstance = reqParams.protocol === 'https:' ? https : http;

	if (!reqParams.host && !reqParams.port) {
		const error = new Error('no active nodes');
		stream.emit('error', error);
		return cb && cb(error);
	}

	var req = requestInstance.request(reqParams, httpResponseHandler.bind(
		this, stream, reqParams, reqData, cb
	));

	req.on('error', function (e) {
		// user should define callback or add event listener for the error event
		if (!cb || (cb && stream.listeners('error').length))
			stream.emit('error', e);
		return cb && cb(e);
	});

	req.on('timeout', function (e) {
		if (!cb || (cb && stream.listeners('error').length))
			stream.emit('error', e);
		req.abort();
	})

	stream.req = req;

	if (reqData.query)
		req.write(reqData.query);

	if (reqData.finalized) {
		req.end();
	}

	return stream;
}

function httpRequestForCluster(
	startParams,
	getReqParams,
	setClusterState,
	reqData,
	retry,
	result,
	cb
) {
	return new Promise(function (resolve, reject) {
		if (!startParams.host || !startParams.port) {
			console.error('No active nodes');
			reject('No active nodes');
		}
		var reqResult = httpRequest(startParams, reqData, cb);
		reqResult.on('error', function (e) {
			if (retry) {
				reqResult.req.abort();
				setClusterState({
					nodes: [{
						host: startParams.host,
						port: startParams.port,
						active: false,
						last: true
					}]
				});
				var newNodeParams = getReqParams();
				var newReqParams = Object.assign({}, startParams, {
					host: newNodeParams.host,
					port: newNodeParams.port,
					path: newNodeParams.path
				});
				httpRequestForCluster(
					newReqParams,
					getReqParams,
					setClusterState,
					reqData,
					retry - 1,
					reqResult,
					cb
				).then(function (result) {
					resolve(result);
				}).catch(function () {
					return reject(result || reqResult);
				});
			} else {
				return reject(result || reqResult);
			}
		});
		reqResult.req.on('response', function () {
			setClusterState({
				nodes: [{
					host: startParams.host,
					port: startParams.port,
					active: true,
					last: true
				}]
			});
			resolve(reqResult);
		})
	});
}

function ClickHouse(options) {
	this.clusterState = {
		nodes: [],
		timers: [],
		retry: 0,
		healthCheckTimeout: 2000
	}
	if (!options) {
		console.error('You must provide at least host name to query ClickHouse');
		return null;
	}

	if (options.constructor === String) {
		options = {
			host: options
		};
	}

	if (options.cluster && options.cluster.constructor === Object && options.cluster.nodes && options.cluster.nodes.length > 0) {
		if (options.cluster.retry) {
			this.clusterState.retry = options.cluster.retry;
		}
		if (options.cluster.healthCheckTimeout) {
			this.clusterState.healthCheckTimeout = options.cluster.healthCheckTimeout;
		}
		options.cluster.nodes.forEach(function (urlObject) {
			const node = Object.assign({}, urlObject, {
				active: false,
				last: false
			});
			this.clusterState.nodes.push(node);
			this.clusterState.timers.push(setInterval(() => {
				this.pinging(node).then(() => {
					node.active = true;
				}).catch(() => {
					node.active = false;
				});
			}, this.clusterState.healthCheckTimeout));
		}, this)
	}

	this.options = options;
}

ClickHouse.prototype.setClusterState = function (state) {
	if (state.nodes && state.nodes.length > 0) {
		state.nodes.forEach(function (node) {
			this.clusterState.nodes.forEach(function (clusterNode) {
				if (clusterNode.host === node.host && clusterNode.port === node.port) {
					clusterNode.active = node.active;
					clusterNode.last = node.last;
				} else {
					clusterNode.last = false;
				}
			}, this);
		}, this);
	}
	if (state.retry) {
		this.clusterState.retry = state.retry;
	}
}

ClickHouse.prototype.getReqParams = function () {
	var urlObject = {};

	// avoid to set defaults - node http module is not happy
	"protocol auth host hostname port path localAddress headers agent createConnection timeout".split(" ").forEach(function (k) {
		if (this.options[k] !== undefined)
			urlObject[k] = this.options[k];
	}, this);

	urlObject.method = 'POST';

	urlObject.path = urlObject.path || '/';

	urlObject.port = urlObject.port || 8123;

	if (this.clusterState.nodes.length > 0) {
		const last = this.clusterState.nodes.find(function (node) {
			return node.last
		});
		if (last) {
			const lastIndex = this.clusterState.nodes.indexOf(last);
			const target = this.clusterState.nodes.find(function (node, i) {
				return node.active && i > lastIndex
			});
			if (target) {
				this.setClusterState({
					nodes: [{
						host: target.host,
						port: target.port,
						last: true,
						active: true
					}]
				});
				urlObject.host = target.host;
				urlObject.port = target.port;
			} else {
				const target = this.clusterState.nodes.find(function (node, i) {
					return node.active && i < lastIndex
				});
				if (target) {
					this.setClusterState({
						nodes: [{
							host: target.host,
							port: target.port,
							last: true,
							active: true
						}]
					});
					urlObject.host = target.host;
					urlObject.port = target.port;
				} else {
					if (last.active) {
						this.setClusterState({
							nodes: [{
								host: last.host,
								port: last.port,
								last: true,
								active: true
							}]
						});
						urlObject.host = last.host;
						urlObject.port = last.port;
					} else {
						urlObject.host = null;
						urlObject.port = null;
					}
				}
			}
		} else {
			const target = this.clusterState.nodes.find(function (node, i) {
				return node.active
			});
			if (target) {
				this.setClusterState({
					nodes: [{
						host: target.host,
						port: target.port,
						last: true,
						active: true
					}]
				});
				urlObject.host = target.host;
				urlObject.port = target.port;
			} else {
				urlObject.host = null;
				urlObject.port = null;
			}
		}
	}
	return urlObject;
}

ClickHouse.prototype.query = function (chQuery, options, cb) {

	chQuery = chQuery.trim();

	if (cb === undefined && options && options.constructor === Function) {
		cb = options;
		options = undefined;
	}

	if (!options)
		options = {
			queryOptions: {}
		};

	options.omitFormat = options.omitFormat || this.options.omitFormat || false;
	options.dataObjects = options.dataObjects || this.options.dataObjects || false;
	options.format = options.format || this.options.format || null;

	// we're adding `queryOptions` passed for constructor if any
	var queryObject = Object.assign({}, this.options.queryOptions, options.queryOptions);

	var formatRegexp = /FORMAT\s+(BlockTabSeparated|CSV|CSVWithNames|JSON|JSONCompact|JSONEachRow|Native|Null|Pretty|PrettyCompact|PrettyCompactMonoBlock|PrettyNoEscapes|PrettyCompactNoEscapes|PrettySpaceNoEscapes|PrettySpace|RowBinary|TabSeparated|TabSeparatedRaw|TabSeparatedWithNames|TabSeparatedWithNamesAndTypes|TSKV|Values|Vertical|XML)/i;
	var formatMatch = chQuery.match(formatRegexp);

	if (!options.omitFormat && formatMatch) {
		options.format = formatMatch[1];
		options.omitFormat = true;
	}

	var reqData = {
		syncParser: options.syncParser || this.options.syncParser || false,
		finalized: true, // allows to write records into connection stream
	};

	var reqParams = this.getReqParams();

	var formatEnding = '';

	// format should be added for data queries
	if (chQuery.match(/^(?:SELECT|SHOW|DESC|DESCRIBE|EXISTS\s+TABLE)/i)) {
		if (!options.format)
			options.format = options.dataObjects ? 'JSON' : 'JSONCompact';
	} else if (chQuery.match(/^INSERT/i)) {

		// There is some variants according to the documentation:
		// 1. Values already available in the query: INSERT INTO t VALUES (1),(2),(3)
		// 2. Values must me provided with POST data: INSERT INTO t VALUES
		// 3. Same as previous but without VALUES keyword: INSERT INTO t FORMAT Values
		// 4. Insert from SELECT: INSERT INTO t SELECT…

		// we need to handle 2 and 3 and http stream must stay open in that cases
		if (chQuery.match(/\s+VALUES\b/i)) {
			if (chQuery.match(/\s+VALUES\s*$/i))
				reqData.finalized = false;

			options.format = 'Values';
			options.omitFormat = true;

		} else if (chQuery.match(/INSERT\s+INTO\s+\S+\s+(?:\([^\)]+\)\s+)?SELECT/mi)) {
			reqData.finalized = true;
			options.omitFormat = true;
		} else {

			reqData.finalized = false;

			if (!chQuery.match(/FORMAT/i)) {
				// simplest format to use, only need to escape \t, \\ and \n
				options.format = options.format || 'TabSeparated';
				formatEnding = ' '; // clickhouse don't like data immediately after format name
			} else {

			}
		}
	} else {
		options.omitFormat = true;
	}

	reqData.format = options.format;

	// use query string to submit ClickHouse query — useful to mock CH server
	if (this.options.useQueryString) {
		queryObject.query = chQuery + ((options.omitFormat) ? '' : ' FORMAT ' + options.format + formatEnding);
		reqParams.method = 'GET';
	} else {
		reqData.query = chQuery + (options.omitFormat ? '' : ' FORMAT ' + options.format + formatEnding);
		reqParams.method = 'POST';
	}

	reqParams.query = queryObject;
	if (this.clusterState.nodes.length > 0) {
		var getReqParams = this.getReqParams.bind(this);
		var setClusterState = this.setClusterState.bind(this);
		var stream = httpRequestForCluster(reqParams, getReqParams, setClusterState, reqData, this.clusterState.retry, null, cb);

		return stream;
	} else {
		var stream = httpRequest(reqParams, reqData, cb);

		return stream;
	}
}

ClickHouse.prototype.querying = function (chQuery, options) {

	return new Promise(function (resolve, reject) {
		// Force override `syncParser` option when using promise api
		const queryOptions = Object.assign({}, options, {
			syncParser: true
		})
		var stream = this.query(chQuery, queryOptions, function (err, data) {
			if (err)
				return reject(err);
			resolve(data);
		});
	}.bind(this));
}

ClickHouse.prototype.ping = function (cb) {

	var reqParams = this.getReqParams();

	reqParams.method = 'GET';

	var stream = httpRequest(reqParams, {
		finalized: true
	}, cb);

	return stream;
}

ClickHouse.prototype.pinging = function (params) {
	return new Promise(function (resolve, reject) {
		var reqParams = {};
		if (params && params.constructor === Object && params.host && params.port) {
			reqParams.host = params.host;
			reqParams.port = params.port;
		} else {
			reqParams = this.getReqParams();
		}

		reqParams.method = 'GET';

		httpRequest(reqParams, {
			finalized: true
		}, function (err, data) {
			if (err)
				return reject(err);
			resolve(data);
		});
	}.bind(this));
}

ClickHouse.prototype.getClusterStatus = function () {
	return this.clusterState.nodes;
}

ClickHouse.prototype.checkCluster = function () {
	return Promise.all(this.clusterState.nodes.map(function (node) {
		return this.pinging({
			host: node.host,
			port: node.port
		}).then(() => {
			node.active = true;
		}).catch(() => {
			node.active = false;
		});
	}, this));
}

module.exports = ClickHouse;