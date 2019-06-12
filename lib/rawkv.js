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
    let region;
    let peer;

    return this
      ._locateKey(key)
      .then(response => {
        region = response.region;
        let leader = response.leader;

        // region 是否需要只取 peers[0]
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

        // tikvpb.RawPut({context, key, value, cf});
      })
      .then(response => {
        let { store } = response;
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
        return response.value.toString();
      });
  }

  put(key, value) {
    let peer;
    let region;

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

  // delete(key) {}

  // batchGet(keys) {}
  // batchPut(keys, values) {}
  // batchDelete(keys) {}
  // deleteRange(start, end) {}

  // scan(start, end, limit) {}
  // reverseScan(start, end, limit) {}

  // @param key is the key of the item stored
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
