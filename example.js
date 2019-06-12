const newRawKvClient = require('.').rawkv;

let address = 'pd0:2379';

(async () => {
  let client = await newRawKvClient(address);

  let id = client.clusterId;
  console.log('Cluster ID:', id.toString());

  await client.put('company', 'PingCAP');
  let company = await client.get('company');
  console.log('Company:', company);
})();
