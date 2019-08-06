const newRawKvClient = require('.').rawkv;

let address = 'pd0:2379';

(async () => {
  let client = await newRawKvClient(address);

  let id = client.clusterId;
  console.log('Cluster ID:', id.toString());

  await client.put('company', 'PingCAP');
  await client.put('fruit', 'Apple');

  let company = await client.get('company');
  console.log('Get company:', company);

  let fruit = await client.get('fruit');
  console.log('Get fruit:', fruit);

  let values = await client.batchGet(['company', 'fruit']);
  console.log('BatchGet:', values);

  await client.delete('company');
  await client.delete('fruit');
})();
