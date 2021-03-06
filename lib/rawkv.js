const { promisify } = require('util');
const url = require('url');

const protoLoader = require('@grpc/proto-loader');
const grpc = require('grpc');
const { pdpb } = grpc.loadPackageDefinition(protoLoader.loadSync(__dirname + '/../proto/pdpb.proto'));
const { tikvpb } = grpc.loadPackageDefinition(protoLoader.loadSync(__dirname + '/../proto/tikvpb.proto'));
const TikvClient = tikvpb.Tikv;

const MaxScanLimit = 10240;
const MaxBatchPutSize = 16 * 1024;
const BatchPairCount = 512;

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

  batchDelete(keys) {
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

          return promisify(client.rawBatchDelete).bind(client)({
            context: {
              regionId: region.id,
              regionEpoch: region.regionEpoch,
              peer,
            },
            keys: keys.map(str => Buffer.from(str)),
          });
        };

        return Promise.all(Object.values(groups).map(sendRequest));
      });
  }

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
            if (response.pairs) {
              return response.pairs.map(p => ({
                  key: p.key.toString(),
                  value: p.value.toString() ,
                })
              );
            }

            return [];
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

  // enable tikv config: use-delete-range = true
  async deleteRange(startKey, endKey) {
    if (!endKey) {
      endKey = '';
    }

    const createContext = (region, peer) => {
      return {
        regionId: region.id,
        regionEpoch: region.regionEpoch,
        peer,
      };
    };

    const createTikvClient = (address) => {
      let { credentials } = this._config;
      return new TikvClient(address, credentials);
    };

    const getActualEndKey = (region) => {
      let result = endKey;

      if (region.endKey && region.endKey < result) {
        result = region.endKey;
      }

      return result;
    };

    let sendRequest = async ({region, peer, store}) => {
      let client = createTikvClient(store.address);
      let actualEndKey = getActualEndKey(region);

      let response = await promisify(client.rawDeleteRange).bind(client)({
        context: createContext(region, peer),
        startKey: Buffer.from(startKey),
        endKey: Buffer.from(actualEndKey),
      });

      if (response.regionError) {
        throw response.regionError;
      }

      if (response.error) {
        throw response.error;
      }

      return actualEndKey;

    };

    while (startKey != endKey) {
      // get store based on start key
      let info = await this._getStore(startKey);

      // send delete range request
      startKey = await sendRequest(info);
    }

  }

  //TODO add @param limit
  scan(startKey, endKey) {
    if (!endKey) {
      endKey = '';
    }

    return this
      ._getStore(startKey)
      .then(({region, peer, store}) => {
        let { credentials } = this._config;

        let client = new TikvClient(store.address, credentials);

        return promisify(client.rawScan).bind(client)({
          context: {
            regionId: region.id,
            regionEpoch: region.regionEpoch,
            peer,
          },
          startKey: Buffer.from(startKey),
          endKey: Buffer.from(endKey),
          limit: MaxScanLimit,
        });
      })
      .then(response => {
        if (response.kvs) {
          return response.kvs.map(p => ({
              key: p.key.toString(),
              value: p.value.toString() ,
            })
          );
        }
        else {
          return [];
        }
      });
  }

  reverseScan(startKey, endKey) {
    if (!endKey) {
      endKey = '';
    }

    return this
      ._getStore(startKey)
      .then(({region, peer, store}) => {
        let { credentials } = this._config;

        let client = new TikvClient(store.address, credentials);

        return promisify(client.rawScan).bind(client)({
          context: {
            regionId: region.id,
            regionEpoch: region.regionEpoch,
            peer,
          },
          startKey: Buffer.from(startKey),
          endKey: Buffer.from(endKey),
          limit: MaxScanLimit,
          reverse: true,
        });
      })
      .then(response => {
        if (response.kvs) {
          return response.kvs.map(p => ({
              key: p.key.toString(),
              value: p.value.toString() ,
            })
          );
        }
        else {
          return [];
        }
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
            resolve({
              region: response.region,
              leader: response.leader,
            });
          }
        }
      );
    });
  }

  _getStore(key) {
    return this
      ._locateKey(key)
      .then(({region, leader}) => {

        // assign peers[0] to peer directly
        // https://sourcegraph.com/github.com/tikv/client-go/-/blob/locate/region_cache.go#L351
        let peer = region.peers[0];

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
        ).then(({ store }) => {
          return { region, peer, store, key };
        });
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
