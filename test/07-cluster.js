var ClickHouse = require("../src/clickhouse");

var http = require('http');
var url = require('url');
var qs = require('querystring');

var assert = require("assert");

var responses = {
  "SELECT number FROM system.numbers LIMIT 10 FORMAT JSONCompact": {
    "meta": [{
      "name": "1",
      "type": "UInt8"
    }],
    "rows": 1
  },
  "INSERT INTO t VALUES (1),(2),(3)": {
    "meta": [{
      "name": "1",
      "type": "UInt8"
    }],
    "rows": 1
  },
};

function createServer(serverId) {
  var server = http.createServer(function (req, res) {
    var queryString = url.parse(req.url).query;

    // test only supports db queries using queryString
    if (!queryString) {
      res.writeHead(200, {});
      res.end("Ok.\n");
      return;
    }

    var queryObject = qs.parse(queryString);
    if (queryObject.query in responses) {
      res.writeHead(200, {
        "Content-Type": "application/json; charset=UTF-8",
      });
      // console.log (JSON.stringify (responses[queryObject.query], null, "\t"));
      res.end(JSON.stringify({
        ...responses[queryObject.query],
        data: [serverId]
      }, null, "\t"));
      return;
    }

    res.writeHead(500, {
      "Content-Type": "text/plain; charset=UTF-8",
      node: serverId
    });
    res.end("Simulated error");
  });

  return server;
}

describe("simulated queries on cluster", function () {

  var server1, server2, server3,
    host1, host2, host3,
    port1, port2, port3, servers;
  before(async function (done) {

    server1 = createServer('1');
    server2 = createServer('2');
    server3 = createServer('3');

    server1.on('clientError', function (err, socket) {
      socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    });
    server2.on('clientError', function (err, socket) {
      socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    });
    server3.on('clientError', function (err, socket) {
      socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    });

    servers = [server1, server2, server3];

    await Promise.all(servers.map((server, i) => {
      return new Promise(resolve => {
        server.listen(0, function (evt) {
          if (i === 0) {
            host1 = server.address().address;
            port1 = server.address().port;

            host1 = host1 === '0.0.0.0' ? '127.0.0.1' : host1;
            host1 = host1 === '::' ? '127.0.0.1' : host1;
          } else if (i === 1) {
            host2 = server.address().address;
            port2 = server.address().port;

            host2 = host2 === '0.0.0.0' ? '127.0.0.1' : host2;
            host2 = host2 === '::' ? '127.0.0.1' : host2;
          } else {
            host3 = server.address().address;
            port3 = server.address().port;

            host3 = host3 === '0.0.0.0' ? '127.0.0.1' : host3;
            host3 = host3 === '::' ? '127.0.0.1' : host3;
          }
          resolve();
        });
      })
    }));

    done();
  })

  after(async function (done) {
    await Promise.all(servers.map(server => {
      return new Promise(resolve => {
        server.close(function () {
          resolve();
        });
      });
    }));
    done();
  });

  // this.timeout (5000);

  it("300 requests. Requests should be distributed evenly", async function (done) {
    var ch = new ClickHouse({
      cluster: {
        nodes: [{
          host: host1,
          port: port1
        }, {
          host: host2,
          port: port2
        }, {
          host: host3,
          port: port3
        }],
        retry: 5 
      },
      useQueryString: true,
    });
    await ch.checkCluster();

    const streams = [];
    let count = 300;
    while (count) {
      streams.push(new Promise(async resolve => {
        const query = [
          'SELECT number FROM system.numbers LIMIT 10',
          'INSERT INTO t VALUES (1),(2),(3)']
          [Math.round(Math.random())];
        const stream = await ch.query(query);
        stream.on('data', (data) => {
          resolve(data);
        });
      }));
      count = count - 1;
    }
    const results = await Promise.all(streams);

    const total = results.reduce((acc, item) => {
      if (item === '1') {
        return {
          server1: acc.server1 + 1,
          server2: acc.server2,
          server3: acc.server3,
        }
      } else if (item === '2') {
        return {
          server1: acc.server1,
          server2: acc.server2 + 1,
          server3: acc.server3,
        }
      } else {
        return {
          server1: acc.server1,
          server2: acc.server2,
          server3: acc.server3 + 1,
        }
      }
    }, {
      server1: 0,
      server2: 0,
      server3: 0
    });

    assert(total.server1 === 100);
    assert(total.server2 === 100);
    assert(total.server3 === 100);
    done();
  });

  it("300 requests. Crash one server", async function (done) {
    var ch = new ClickHouse({
      cluster: {
        nodes: [{
          host: host1,
          port: port1
        }, {
          host: host2,
          port: port2
        }, {
          host: host3,
          port: port3
        }],
        retry: 5 
      },
      useQueryString: true,
    });
    await ch.checkCluster();

    const streams = [];
    const totalRequest = 300;
    let count = totalRequest;
    while (count) {
      streams.push(new Promise(async resolve => {
        const query = [
          'SELECT number FROM system.numbers LIMIT 10',
          'INSERT INTO t VALUES (1),(2),(3)']
          [Math.round(Math.random())];
        const stream = await ch.query(query);
        stream.on('data', (data) => {
          resolve(data);
        });
      }));
      count = count - 1;
    }

    setTimeout(() => {
      console.log(`Disable server: ${server1.address().port}`);
      server1.close();
    }, 80);

    const results = await Promise.all(streams);
    assert(results.length === totalRequest);
    done();
  });


});