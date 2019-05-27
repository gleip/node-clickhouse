const Clickhouse = require('./src/clickhouse');

(async () => {
  const connect = new Clickhouse({
    clusterNodes: [{
      host: 'localhost',
      port: 8123
    }, {
      host: 'localhost',
      port: 8124
    }, {
      host: 'localhost',
      port: 8125
    }],
    clusterRetry: 1,
    healthCheckTimeout: 2000
  });
  console.log('start --->', JSON.stringify(connect.getClusterStatus(), null, 2));
  await connect.checkCluster();
  console.log('finish --->', JSON.stringify(connect.getClusterStatus(), null, 2));
  setInterval(() => console.log(JSON.stringify(connect.getClusterStatus(), null, 2)), 1000);
  setInterval(() => connect.query('SELECT now()'), 4000);
})();