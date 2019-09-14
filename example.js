const newRawKvClient = require('.').rawkv;

let address = 'pd0:2379';

(async () => {
  let client = await newRawKvClient(address);

  console.log(`Cluster ID: ${client.clusterId}\n`);

  await client.put('company', 'PingCAP');
  console.log(`key "company" should have value: ${await client.get('company')}\n`);

  await client.delete('company');
  console.log(`after delete, key "company" should have null value: ${await client.get('company')}\n`);

  await client.batchPut({
      company: 'PingCAP',
      fruit: 'Apple',
      baby: 'Ruby',
    }
  );
  console.log('batchGet "fruit", "company" should have values:');
  for (let kv of await client.batchGet(['fruit', 'company'])) {
    console.log(kv.key, '=', kv.value);
  }
  console.log('');


  await client.batchDelete(['company', 'fruit']);
  console.log('after batchDelete "fruit", "company" should have empty values:', await client.batchGet(['fruit', 'company']));
  console.log('');

  await client.batchPut({
    'cloud provider': 'AWS',
    'company': 'Apple',
    'framework': 'Spring',
    'lang': 'Java',
    'method': 'Agile',
    'orchestration': 'kubernetes',
    'practice': 'DevOps',
    'runtime': 'docker',
  });


  console.log('scan from "c" to "f" should return all the keys begin with "c"');
  for (let kv of await client.scan('c', 'f')) {
    console.log(kv.key, '=', kv.value);
  }
  console.log('');

  console.log('reverseScan from "g" to "c" should return all the keys begin with "f" or "c":');
  for (let kv of await client.reverseScan('g', 'c')) {
    console.log(kv.key, '=', kv.value);
  }
  console.log('');

  console.log('deleteRange from "m" should keep only the keys begin with "b" or "c":');
  await client.deleteRange('f');
  for (let kv of await client.scan('a', 'z')) {
    console.log(kv.key, '=', kv.value);
  }
  console.log('');

})();
