const { promisify } = require('util');
const url = require('url');

const protoLoader = require('@grpc/proto-loader');
const grpc = require('grpc');
const { pdpb } = grpc.loadPackageDefinition(protoLoader.loadSync(__dirname + '/../proto/pdpb.proto'));
const { tikvpb } = grpc.loadPackageDefinition(protoLoader.loadSync(__dirname + '/../proto/tikvpb.proto'));
const TikvClient = tikvpb.Tikv;

class Client {

  constructor(pdClient, clusterInfo, config) {
    this._pdClient = pdClient;
    this._clusterInfo = clusterInfo;
    this._config = config;
    this.connections = {};
  }

  get clusterId() { return this._clusterInfo.id; }
  get leader() { return this._clusterInfo.leader; }
  get members() { return this._clusterInfo.members; }

  get leaderPD() {
    // TODO update leader on demand
    if (!this.connections.leaderPD) {
      let { credentials } = this._config;
      // TODO reuse one RPC client of leader
      let host = url.parse(this.leader.clientUrls[0]).host;
      this.connections.leaderPD = new pdpb.PD(host, credentials);
    }

    return this.connections.leaderPD;
  }

  get(key) {
    return this
      ._getStore(key)
      .then(({region, peer, store}) => {
        let { credentials } = this._config;

        // addr, region, peer are all from RPCContext
        // https://sourcegraph.com/github.com/tikv/client-go/-/blob/locate/region_cache.go#L93
        let client = new TikvClient(store.address, credentials);

        return promisify(client.rawGet).bind(client)({
          context: {
            regionId: region.id,
            regionEpoch: region.regionEpoch,
            peer,
          },
          key: Buffer.from(key),
        });
      }).
      then(response => {
        if (response.value) {
          return response.value.toString();
        }

        return null;
      });
  }

  put(key, value) {
    return this
      ._getStore(key)
      .then(({region, peer, store}) => {
        let { credentials } = this._config;

        let client = new TikvClient(store.address, credentials);

        return promisify(client.rawPut).bind(client)({
          context: {
            regionId: region.id,
            regionEpoch: region.regionEpoch,
            peer,
          },
          key: Buffer.from(key),
          value: Buffer.from(value),
        });
      });
  }

  delete(key) {
    return this
      ._getStore(key)
      .then(({region, peer, store}) => {
        let { credentials } = this._config;

        let client = new TikvClient(store.address, credentials);

        return promisify(client.RawDelete).bind(client)({
          context: {
            regionId: region.id,
            regionEpoch: region.regionEpoch,
            peer,
          },
          key: Buffer.from(key),
        });
      });
  }

  // @param pairs is a map of {key: value}
  batchPut(pairs) {
    let keys = Object.keys(pairs);

    return Promise
      .all(keys.map(key => this._getStore(key)))
      .then(regions => {
        let groups = regions.reduce((accum, v) => {
          const { region, peer, store, key } = v;
          let g = accum[region.Id];

          if (g) {
            g.pairs.push({
              key: Buffer.from(key),
              value: Buffer.from(pairs[key]),
            });
          }
          else {
            g = {
              region,
              peer,
              store,
              pairs: [
                {
                  key: Buffer.from(key),
                  value: Buffer.from(pairs[key]),
                }
              ],
            };
            accum[region.Id] = g;
          }

          return accum;
        }, {});

        let sendRequest = ({region, peer, store, pairs}) => {
          let { credentials } = this._config;

          //TODO reuse client
          let client = new TikvClient(store.address, credentials);

          return promisify(client.rawBatchPut).bind(client)({
            context: {
              regionId: region.id,
              regionEpoch: region.regionEpoch,
              peer,
            },
            pairs
          });
        };

        return Promise.all(Object.values(groups).map(sendRequest));
      });
  }

  // batchDelete(keys) {}
  // deleteRange(start, end) {}

  // scan(start, end, limit) {}
  // reverseScan(start, end, limit) {}

  batchGet(keys) {
    return Promise
      .all(keys.map(key => this._getStore(key)))
      .then(regions => {

        let groups = regions.reduce((accum, v) => {
          const { region, peer, store, key } = v;
          let g = accum[region.Id];

          if (g) {
            g.keys.push(key);
          }
          else {
            g = { region, peer, store, keys: [key] };
            accum[region.Id] = g;
          }

          return accum;
        }, {});

        let sendRequest = ({region, peer, store, keys}) => {
          let { credentials } = this._config;

          //TODO reuse client
          let client = new TikvClient(store.address, credentials);

          return promisify(client.rawBatchGet).bind(client)({
            context: {
              regionId: region.id,
              regionEpoch: region.regionEpoch,
              peer,
            },
            keys: keys.map(str => Buffer.from(str)),
          })
          .then(response => {
            return response.pairs.map(p => ({
                key: p.key.toString(),
                value: p.value.toString() ,
              })
            );
          });
        };

        return Promise
          .all(Object.values(groups).map(sendRequest))
          .then(nestedPairs => {
            return nestedPairs.reduce((result, pairs) => {
              return result.concat(pairs);
            }, []);
          });
      });
  }

  // TODO the region returned should be cached
  _locateKey(key) {
    return new Promise((resolve, reject) => {
      this.leaderPD.getRegion(
        {
          header: {
            clusterId: this.clusterId,
          },
          region_key: key,
        },
        (err, response) => {
          if (err) {
            reject(err);
          }
          else {
            resolve(response);
          }
        }
      );
    });
  }

  _getStore(key) {
    let region;
    let peer;
    return this
      ._locateKey(key)
      .then(response => {
        region = response.region;
        let leader = response.leader;

        // assign peers[0] to peer directly
        // https://sourcegraph.com/github.com/tikv/client-go/-/blob/locate/region_cache.go#L351
        peer = region.peers[0];

        if (leader) {
          peer = leader;
          // for (const p of region.peers) {
          //   if (leader.storeId.toString() === p.storeId.toString()) {
          //     peer = p;
          //     break;
          //   }
          // }
        }

        return promisify(this.leaderPD.GetStore).bind(this.leaderPD)(
          {
            header: {
              clusterId: this.clusterId,
            },
            storeId: peer.storeId,
          }
        );
      })
      .then(response => {
        let { store } = response;
        return { region, peer, store, key };
      });
  }
}

function initClient(pdClient) {
  return new Promise((resolve, reject) => {
    pdClient.getMembers(
      {
        header: {
          cluster_id: 0
        }
      },
      (err, response) => {
        if (err) {
          reject(err);
        }
        else {
          resolve(response);
        }
      }
    );
  });
}

module.exports = async function newClient(address) {
  let credentials = grpc.credentials.createInsecure();
  let config = {
    address,
    credentials,
  };

  let pdClient = new pdpb.PD(address, credentials);

  let response = await initClient(pdClient);

  let id =  response.header.clusterId;
  let leader = response.leader;
  let members = response.members;

  return new Client(pdClient, {id, leader, members}, config);
};
