import { GossipsubRouter, GossipsubMessage } from '../src/core/gossipsub';
import { MessagingNode } from '../src/node';

describe('Gossipsub PubSub & Peer Discovery (Issue #81)', () => {
  let routerA: GossipsubRouter;
  let routerB: GossipsubRouter;

  beforeEach(() => {
    routerA = new GossipsubRouter('peer-alice', {
      meshDegree: 3,
      meshDegreeLow: 2,
      meshDegreeHigh: 5,
      heartbeatIntervalMs: 100,
      peerTtlMs: 500,
    });

    routerB = new GossipsubRouter('peer-bob', {
      meshDegree: 3,
      meshDegreeLow: 2,
      meshDegreeHigh: 5,
      heartbeatIntervalMs: 100,
      peerTtlMs: 500,
    });
  });

  afterEach(async () => {
    await routerA.stop();
    await routerB.stop();
  });

  describe('Topic Subscription Logic', () => {
    it('subscribes and tracks active topics', async () => {
      const messages: GossipsubMessage[] = [];
      await routerA.start();

      await routerA.subscribe('cross-chain-alerts', (msg) => {
        messages.push(msg);
      });

      expect(routerA.isSubscribed('cross-chain-alerts')).toBe(true);
      expect(routerA.getSubscribedTopics()).toContain('cross-chain-alerts');
      expect(routerA.getSubscribedTopics()).toContain(routerA.discoveryTopic);

      // Publish local message
      const published = await routerA.publish('cross-chain-alerts', JSON.stringify({ alert: 'reorg-detected' }));
      expect(published.topic).toBe('cross-chain-alerts');
      expect(messages.length).toBe(1);
      expect(messages[0].id).toBe(published.id);
    });

    it('unsubscribes from topics correctly', async () => {
      await routerA.start();
      const fn = jest.fn();

      await routerA.subscribe('test-topic', fn);
      expect(routerA.isSubscribed('test-topic')).toBe(true);

      await routerA.unsubscribe('test-topic', fn);
      expect(routerA.isSubscribed('test-topic')).toBe(false);
      expect(routerA.getSubscribedTopics()).not.toContain('test-topic');
    });

    it('deduplicates incoming messages with identical IDs', async () => {
      await routerA.start();
      let callCount = 0;
      await routerA.subscribe('news', () => {
        callCount++;
      });

      const sampleMsg: GossipsubMessage = {
        id: 'msg-hash-12345',
        topic: 'news',
        from: 'peer-charlie',
        data: 'stellar protocol 21 upgrade scheduled',
        seqno: 1,
        timestamp: Date.now(),
      };

      const acceptedFirst = routerA.handleIncomingMessage('peer-charlie', sampleMsg);
      const acceptedSecond = routerA.handleIncomingMessage('peer-charlie', sampleMsg);

      expect(acceptedFirst).toBe(true);
      expect(acceptedSecond).toBe(false);
      expect(callCount).toBe(1);
    });
  });

  describe('Peer Connection / Disconnection Lifecycle', () => {
    it('handles peer connect and disconnect events', (done) => {
      routerA.start().then(() => {
        let connectedFired = false;

        routerA.on('peer:connected', (peerId, address) => {
          expect(peerId).toBe('peer-bob');
          expect(address).toBe('ws://127.0.0.1:9001');
          connectedFired = true;
        });

        routerA.on('peer:disconnected', (peerId, reason) => {
          expect(peerId).toBe('peer-bob');
          expect(reason).toBe('transport_closed');
          expect(connectedFired).toBe(true);
          expect(routerA.getConnectedPeers()).not.toContain('peer-bob');
          done();
        });

        routerA.handlePeerConnected('peer-bob', 'ws://127.0.0.1:9001');
        expect(routerA.getConnectedPeers()).toContain('peer-bob');

        routerA.handlePeerDisconnected('peer-bob', 'transport_closed');
      });
    });
  });

  describe('Automatic Peer Discovery via Discovery Beacon', () => {
    it('automatically discovers peers through gossipsub discovery beacon', async () => {
      await routerA.start();
      await routerB.start();

      let discoveredEventCalled = false;
      routerA.on('peer:discovered', (peer) => {
        if (peer.peerId === 'peer-bob') {
          discoveredEventCalled = true;
        }
      });

      // Bob emits discovery beacon
      const beaconMsg = await routerB.broadcastDiscoveryBeacon('ws://127.0.0.1:9002');

      // Alice receives discovery beacon
      const processed = routerA.handleIncomingMessage('peer-bob', beaconMsg);
      expect(processed).toBe(true);
      expect(discoveredEventCalled).toBe(true);
      expect(routerA.getConnectedPeers()).toContain('peer-bob');
    });
  });

  describe('Gossipsub Mesh Balancing (Graft & Prune)', () => {
    it('grafts peer into topic mesh and balances degree during heartbeat', async () => {
      await routerA.start();

      // Connect 4 peers who subscribe to 'orders'
      for (let i = 1; i <= 4; i++) {
        const id = `peer-order-${i}`;
        routerA.handlePeerConnected(id);
        routerA.updatePeerSubscriptions(id, ['orders']);
      }

      // Alice subscribes to 'orders'
      await routerA.subscribe('orders');

      // Run heartbeat
      routerA.heartbeat();

      const meshPeers = routerA.getMeshPeers('orders');
      expect(meshPeers.length).toBeGreaterThanOrEqual(2);
      expect(meshPeers.length).toBeLessThanOrEqual(4);
    });
  });

  describe('MessagingNode Integration', () => {
    it('MessagingNode exposes gossipsub and supports pubsub subscribe/publish', async () => {
      const node = new MessagingNode(9876);
      expect(node.gossipsub).toBeDefined();

      let received = false;
      await node.subscribe('stellar-txs', (msg) => {
        expect(msg.data).toBe('tx-hash-0xabc');
        received = true;
      });

      await node.publish('stellar-txs', 'tx-hash-0xabc');
      expect(received).toBe(true);

      await node.close();
    });
  });
});
