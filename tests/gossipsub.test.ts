import { GossipSubService, PubSubMessage, PeerDiscoveryAnnouncement } from '../src/core/gossipsub';
import { MessagingNode } from '../src/node';

describe('GossipSubService Protocol & Peer Discovery', () => {
  let service: GossipSubService;

  beforeEach(() => {
    service = new GossipSubService({
      peerId: 'local-node-1',
      D: 3,
      D_low: 2,
      D_high: 6,
      heartbeatIntervalMs: 50,
      listenAddresses: ['ws://127.0.0.1:9001'],
    });
  });

  afterEach(() => {
    service.stop();
  });

  describe('Configuration & Initialization', () => {
    it('initializes with default and custom Gossipsub parameters', () => {
      expect(service.peerId).toBe('local-node-1');
      expect(service.config.D).toBe(3);
      expect(service.config.D_low).toBe(2);
      expect(service.config.D_high).toBe(6);
      expect(service.config.heartbeatIntervalMs).toBe(50);
      expect(service.config.discoveryTopic).toBe('web3-msg/peer-discovery/v1.0.0');
    });

    it('automatically subscribes to the discovery topic on startup', () => {
      expect(service.isSubscribed(service.config.discoveryTopic)).toBe(true);
      expect(service.getSubscriptions()).toContain(service.config.discoveryTopic);
    });
  });

  describe('Topic Subscription Logic', () => {
    it('subscribes to custom topics and manages subscription list', () => {
      service.subscribe('payments.stellar');
      service.subscribe('orders.settlement');

      expect(service.isSubscribed('payments.stellar')).toBe(true);
      expect(service.isSubscribed('orders.settlement')).toBe(true);
      expect(service.getSubscriptions()).toEqual(
        expect.arrayContaining(['payments.stellar', 'orders.settlement', service.config.discoveryTopic]),
      );
    });

    it('unsubscribes from topics and prunes mesh', () => {
      service.subscribe('temporary.topic');
      expect(service.isSubscribed('temporary.topic')).toBe(true);

      let pruneEventFired = false;
      service.on('subscription:change', (ev) => {
        if (ev.type === 'unsubscribe' && ev.topic === 'temporary.topic') {
          pruneEventFired = true;
        }
      });

      service.unsubscribe('temporary.topic');
      expect(service.isSubscribed('temporary.topic')).toBe(false);
      expect(pruneEventFired).toBe(true);
    });

    it('delivers published messages to topic subscribers', async () => {
      const received: PubSubMessage[] = [];
      service.subscribe('cross-chain.events', (msg) => {
        received.push(msg);
      });

      await service.publish('cross-chain.events', { amount: '100', asset: 'XLM' });

      expect(received).toHaveLength(1);
      expect(received[0].topic).toBe('cross-chain.events');
      expect(JSON.parse(received[0].data)).toEqual({ amount: '100', asset: 'XLM' });
      expect(received[0].from).toBe('local-node-1');
    });

    it('isolates messages between distinct topics', async () => {
      const topicAMessages: string[] = [];
      const topicBMessages: string[] = [];

      service.subscribe('topic.alpha', (m) => topicAMessages.push(m.data));
      service.subscribe('topic.beta', (m) => topicBMessages.push(m.data));

      await service.publish('topic.alpha', 'alpha-data');
      await service.publish('topic.beta', 'beta-data');

      expect(topicAMessages).toEqual(['alpha-data']);
      expect(topicBMessages).toEqual(['beta-data']);
    });
  });

  describe('Peer Connection & Disconnection Event Handling', () => {
    it('handles peer:connect event and registers peer info', (done) => {
      service.on('peer:connect', (peerId, info) => {
        expect(peerId).toBe('peer-remote-2');
        expect(info.addresses).toContain('ws://127.0.0.1:9002');
        expect(service.getConnectedPeerIds()).toContain('peer-remote-2');
        done();
      });

      service.handlePeerConnected('peer-remote-2', ['ws://127.0.0.1:9002']);
    });

    it('handles peer:disconnect event and cleans peer from mesh', (done) => {
      service.subscribe('shared.mesh.topic');
      service.handlePeerConnected('peer-remote-3', ['ws://127.0.0.1:9003']);

      // Peer was added to mesh
      service.graft('shared.mesh.topic', 'peer-remote-3');
      expect(service.getMeshPeers('shared.mesh.topic')).toContain('peer-remote-3');

      service.on('peer:disconnect', (peerId, reason) => {
        expect(peerId).toBe('peer-remote-3');
        expect(reason).toBe('connection-reset');
        // Must be pruned from topic mesh
        expect(service.getMeshPeers('shared.mesh.topic')).not.toContain('peer-remote-3');
        expect(service.getConnectedPeerIds()).not.toContain('peer-remote-3');
        done();
      });

      service.handlePeerDisconnected('peer-remote-3', 'connection-reset');
    });

    it('maintains mesh degree between D_low and D_high via GRAFT and PRUNE', () => {
      service.subscribe('degree.test.topic');

      // Connect 8 peers
      for (let i = 1; i <= 8; i++) {
        service.handlePeerConnected(`peer-degree-${i}`, [`ws://127.0.0.1:901${i}`]);
      }

      service.heartbeat();
      const meshPeers = service.getMeshPeers('degree.test.topic');
      // Should satisfy D_low <= mesh.length <= D_high
      expect(meshPeers.length).toBeGreaterThanOrEqual(service.config.D_low);
      expect(meshPeers.length).toBeLessThanOrEqual(service.config.D_high);
    });
  });

  describe('Automatic Peer Discovery via PubSub', () => {
    it('discovers remote peers when announcement is received on discovery topic', (done) => {
      const announcement: PeerDiscoveryAnnouncement = {
        type: 'peer_discovery_announcement',
        peerId: 'peer-discovered-99',
        addresses: ['ws://192.168.1.150:8080'],
        publicKey: 'GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H',
        topics: ['payments.stellar'],
        timestamp: Date.now(),
      };

      service.on('peer:discovered', (discovered) => {
        expect(discovered.peerId).toBe('peer-discovered-99');
        expect(discovered.addresses).toContain('ws://192.168.1.150:8080');
        expect(discovered.topics).toContain('payments.stellar');

        const peerInfo = service.getPeerInfo('peer-discovered-99');
        expect(peerInfo).toBeDefined();
        expect(peerInfo?.addresses).toContain('ws://192.168.1.150:8080');
        done();
      });

      service.handleIncomingMessage({
        msgId: 'announcement-msg-1',
        topic: service.config.discoveryTopic,
        data: JSON.stringify(announcement),
        from: 'peer-discovered-99',
        seqno: 1,
        timestamp: Date.now(),
      });
    });

    it('emits peer:autoconnect when autoConnect is true for newly discovered peer', (done) => {
      service.on('peer:autoconnect', ({ peerId, address }) => {
        expect(peerId).toBe('peer-auto-100');
        expect(address).toBe('ws://10.0.0.5:8080');
        done();
      });

      const announcement: PeerDiscoveryAnnouncement = {
        type: 'peer_discovery_announcement',
        peerId: 'peer-auto-100',
        addresses: ['ws://10.0.0.5:8080'],
        topics: [],
        timestamp: Date.now(),
      };

      service.handleIncomingMessage({
        msgId: 'msg-auto-100',
        topic: service.config.discoveryTopic,
        data: JSON.stringify(announcement),
        from: 'peer-auto-100',
        seqno: 1,
        timestamp: Date.now(),
      });
    });

    it('deduplicates repeat incoming messages', () => {
      let receiveCount = 0;
      service.subscribe('dedup.topic', () => {
        receiveCount += 1;
      });

      const message: PubSubMessage = {
        msgId: 'fixed-msg-id-12345',
        topic: 'dedup.topic',
        data: 'duplicate-payload',
        from: 'sender-42',
        seqno: 1,
        timestamp: Date.now(),
      };

      // First delivery
      const res1 = service.handleIncomingMessage(message);
      expect(res1).not.toBeNull();

      // Second delivery (duplicate msgId)
      const res2 = service.handleIncomingMessage(message);
      expect(res2).toBeNull();

      expect(receiveCount).toBe(1);
    });
  });
});

describe('MessagingNode with GossipSub Integration', () => {
  let nodeA: MessagingNode;
  let nodeB: MessagingNode;
  const portA = 19101;
  const portB = 19102;

  beforeEach(() => {
    nodeA = new MessagingNode(portA, [], undefined, { heartbeatIntervalMs: 100 });
  });

  afterEach(() => {
    if (nodeA) nodeA.close();
    if (nodeB) nodeB.close();
  });

  it('exposes Gossipsub pubsub service on node instance', () => {
    expect(nodeA.pubsub).toBeDefined();
    expect(nodeA.pubsub.isSubscribed(nodeA.pubsub.config.discoveryTopic)).toBe(true);
  });

  it('allows topic subscribe and publish via node convenience methods', async () => {
    const received: PubSubMessage[] = [];
    nodeA.subscribe('orders.new', (msg) => {
      received.push(msg);
    });

    await nodeA.publish('orders.new', { orderId: 'ord-123' });

    expect(received).toHaveLength(1);
    expect(JSON.parse(received[0].data)).toEqual({ orderId: 'ord-123' });
  });

  it('connects to peer and triggers Gossipsub peer connection lifecycle', (done) => {
    nodeB = new MessagingNode(portB, [], undefined, { heartbeatIntervalMs: 100 });

    nodeA.pubsub.once('peer:connect', (peerId) => {
      expect(peerId).toBeDefined();
      expect(nodeA.getConnectedPeerCount()).toBeGreaterThanOrEqual(1);
      done();
    });

    nodeA.connectToPeer(`ws://127.0.0.1:${portB}`);
  });
});
