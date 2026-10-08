import { EventEmitter } from 'events';
import crypto from 'crypto';
import logger from '../logger';

/**
 * Configuration options for the libp2p Gossipsub pubsub router.
 */
export interface GossipsubConfig {
  /** Target degree of the topic mesh (default: 6) */
  meshDegree?: number;
  /** Lower bound for mesh degree before grafting (default: 4) */
  meshDegreeLow?: number;
  /** Upper bound for mesh degree before pruning (default: 12) */
  meshDegreeHigh?: number;
  /** Gossip emission factor for non-mesh peers (default: 0.25) */
  gossipFactor?: number;
  /** Interval in ms for periodic Gossipsub heartbeat (default: 1000) */
  heartbeatIntervalMs?: number;
  /** Time to live for cached seen message IDs in ms (default: 120_000) */
  seenCacheTtlMs?: number;
  /** Standard topic for automatic peer discovery announcements */
  discoveryTopic?: string;
  /** Inactivity threshold in ms before peer is considered stale (default: 30_000) */
  peerTtlMs?: number;
}

/**
 * Metadata and connectivity details of a known network peer.
 */
export interface PeerInfo {
  peerId: string;
  address?: string;
  subscribedTopics: Set<string>;
  lastSeen: number;
  connected: boolean;
}

/**
 * Gossipsub wire payload message.
 */
export interface GossipsubMessage {
  id: string;
  topic: string;
  from: string;
  data: string;
  seqno: number;
  timestamp: number;
  signature?: string;
}

/**
 * Control message structures for Gossipsub mesh maintenance.
 */
export interface ControlGraft {
  topic: string;
}

export interface ControlPrune {
  topic: string;
}

export interface ControlIHave {
  topic: string;
  messageIds: string[];
}

export interface GossipsubControl {
  graft?: ControlGraft[];
  prune?: ControlPrune[];
  ihave?: ControlIHave[];
}

export type MessageHandler = (message: GossipsubMessage) => void;

/**
 * Event map emitted by the Gossipsub router.
 */
export interface GossipsubEvents {
  'peer:connected': (peerId: string, address?: string) => void;
  'peer:disconnected': (peerId: string, reason?: string) => void;
  'peer:discovered': (peer: PeerInfo) => void;
  'message': (message: GossipsubMessage) => void;
  'subscription:change': (topic: string, subscribed: boolean) => void;
  'mesh:update': (topic: string, meshPeers: string[]) => void;
}

/**
 * Robust libp2p-compatible Gossipsub router implementation providing
 * decentralized peer discovery, topic subscription mesh management,
 * and reliable message dissemination.
 */
export class GossipsubRouter extends EventEmitter {
  public readonly peerId: string;
  public readonly discoveryTopic: string;

  private readonly config: Required<GossipsubConfig>;
  private peers: Map<string, PeerInfo> = new Map();
  private topicSubscriptions: Map<string, Set<MessageHandler>> = new Map();
  private topicMesh: Map<string, Set<string>> = new Map(); // topic -> Set of peerIds
  private seenMessageIds: Map<string, number> = new Map(); // messageId -> timestamp
  private seqnoCounter: number = 0;
  private heartbeatTimer?: NodeJS.Timeout;
  private isRunning: boolean = false;

  constructor(peerId?: string, config: GossipsubConfig = {}) {
    super();
    this.peerId = peerId || `peer-${crypto.randomBytes(8).toString('hex')}`;
    this.discoveryTopic = config.discoveryTopic || '/mesh/discovery/1.0.0';

    this.config = {
      meshDegree: config.meshDegree ?? 6,
      meshDegreeLow: config.meshDegreeLow ?? 4,
      meshDegreeHigh: config.meshDegreeHigh ?? 12,
      gossipFactor: config.gossipFactor ?? 0.25,
      heartbeatIntervalMs: config.heartbeatIntervalMs ?? 1000,
      seenCacheTtlMs: config.seenCacheTtlMs ?? 120_000,
      discoveryTopic: this.discoveryTopic,
      peerTtlMs: config.peerTtlMs ?? 30_000,
    };
  }

  /**
   * Start the Gossipsub router, start heartbeat and subscribe to discovery topic.
   */
  public async start(): Promise<void> {
    if (this.isRunning) return;
    this.isRunning = true;

    // Automatically subscribe to the discovery topic to discover and be discovered
    await this.subscribe(this.discoveryTopic, (msg) => {
      this.handleDiscoveryMessage(msg);
    });

    this.heartbeatTimer = setInterval(() => {
      this.heartbeat();
    }, this.config.heartbeatIntervalMs);

    // Unref so process is not held open in test environments
    if (this.heartbeatTimer.unref) {
      this.heartbeatTimer.unref();
    }

    logger.info({ peerId: this.peerId }, 'GossipsubRouter started');
  }

  /**
   * Stop the Gossipsub router, clear timers and disconnect.
   */
  public async stop(): Promise<void> {
    if (!this.isRunning) return;
    this.isRunning = false;

    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }

    // Mark all peers disconnected
    for (const [peerId, peer] of this.peers.entries()) {
      if (peer.connected) {
        peer.connected = false;
        this.emit('peer:disconnected', peerId, 'router_shutdown');
      }
    }

    this.topicMesh.clear();
    this.seenMessageIds.clear();
    logger.info({ peerId: this.peerId }, 'GossipsubRouter stopped');
  }

  /**
   * Subscribe to a topic with optional message callback.
   */
  public async subscribe(topic: string, handler?: MessageHandler): Promise<void> {
    let handlers = this.topicSubscriptions.get(topic);
    const isNewTopic = !handlers;

    if (!handlers) {
      handlers = new Set<MessageHandler>();
      this.topicSubscriptions.set(topic, handlers);
      if (!this.topicMesh.has(topic)) {
        this.topicMesh.set(topic, new Set());
      }
    }

    if (handler) {
      handlers.add(handler);
    }

    if (isNewTopic) {
      this.graftTopicMesh(topic);
      this.emit('subscription:change', topic, true);
    }
  }

  /**
   * Unsubscribe from a topic or remove a specific handler.
   */
  public async unsubscribe(topic: string, handler?: MessageHandler): Promise<void> {
    const handlers = this.topicSubscriptions.get(topic);
    if (!handlers) return;

    if (handler) {
      handlers.delete(handler);
    }

    if (!handler || handlers.size === 0) {
      this.topicSubscriptions.delete(topic);
      this.pruneTopicMesh(topic);
      this.emit('subscription:change', topic, false);
    }
  }

  /**
   * Check if currently subscribed to a topic.
   */
  public isSubscribed(topic: string): boolean {
    return this.topicSubscriptions.has(topic);
  }

  /**
   * Get list of currently subscribed topics.
   */
  public getSubscribedTopics(): string[] {
    return Array.from(this.topicSubscriptions.keys());
  }

  /**
   * Publish a message to a topic.
   */
  public async publish(topic: string, data: string | Uint8Array): Promise<GossipsubMessage> {
    const payloadStr = typeof data === 'string' ? data : Buffer.from(data).toString('utf-8');
    const seqno = ++this.seqnoCounter;
    const now = Date.now();
    const id = this.computeMessageId(topic, this.peerId, seqno, payloadStr);

    const message: GossipsubMessage = {
      id,
      topic,
      from: this.peerId,
      data: payloadStr,
      seqno,
      timestamp: now,
    };

    this.seenMessageIds.set(id, now);

    // Deliver locally if subscribed
    const handlers = this.topicSubscriptions.get(topic);
    if (handlers) {
      for (const h of handlers) {
        try {
          h(message);
        } catch (err) {
          logger.error({ err, topic }, 'Error in local topic handler');
        }
      }
      this.emit('message', message);
    }

    return message;
  }

  /**
   * Handle incoming raw message from a peer.
   */
  public handleIncomingMessage(senderPeerId: string, message: GossipsubMessage): boolean {
    if (!message || !message.id || !message.topic) {
      return false;
    }

    // Deduplication check
    if (this.seenMessageIds.has(message.id)) {
      return false;
    }

    this.seenMessageIds.set(message.id, Date.now());

    // Update sender peer activity
    this.recordPeerActivity(senderPeerId);

    // Deliver to local subscribers
    const handlers = this.topicSubscriptions.get(message.topic);
    if (handlers) {
      for (const handler of handlers) {
        try {
          handler(message);
        } catch (err) {
          logger.error({ err, topic: message.topic }, 'Error dispatching topic message');
        }
      }
      this.emit('message', message);
    }

    return true;
  }

  /**
   * Handle incoming peer connection.
   */
  public handlePeerConnected(peerId: string, address?: string): void {
    if (peerId === this.peerId) return;

    let peer = this.peers.get(peerId);
    if (!peer) {
      peer = {
        peerId,
        address,
        subscribedTopics: new Set(),
        lastSeen: Date.now(),
        connected: true,
      };
      this.peers.set(peerId, peer);
      this.emit('peer:discovered', peer);
    } else {
      peer.connected = true;
      peer.lastSeen = Date.now();
      if (address) peer.address = address;
    }

    this.emit('peer:connected', peerId, address);
    logger.debug({ peerId, address }, 'Peer connected');
  }

  /**
   * Handle peer disconnection.
   */
  public handlePeerDisconnected(peerId: string, reason: string = 'closed'): void {
    const peer = this.peers.get(peerId);
    if (!peer) return;

    peer.connected = false;

    // Remove peer from all topic meshes
    for (const [topic, mesh] of this.topicMesh.entries()) {
      if (mesh.delete(peerId)) {
        this.emit('mesh:update', topic, Array.from(mesh));
      }
    }

    this.emit('peer:disconnected', peerId, reason);
    logger.debug({ peerId, reason }, 'Peer disconnected');
  }

  /**
   * Register a peer's announced topic subscriptions.
   */
  public updatePeerSubscriptions(peerId: string, topics: string[]): void {
    let peer = this.peers.get(peerId);
    if (!peer) {
      peer = {
        peerId,
        subscribedTopics: new Set(topics),
        lastSeen: Date.now(),
        connected: true,
      };
      this.peers.set(peerId, peer);
      this.emit('peer:discovered', peer);
    } else {
      peer.subscribedTopics = new Set(topics);
      peer.lastSeen = Date.now();
    }

    // Refresh meshes where relevant
    for (const topic of topics) {
      if (this.topicSubscriptions.has(topic)) {
        this.graftTopicMesh(topic);
      }
    }
  }

  /**
   * Broadcast a discovery beacon announcing this node and its subscribed topics.
   */
  public async broadcastDiscoveryBeacon(address?: string): Promise<GossipsubMessage> {
    const beacon = JSON.stringify({
      peerId: this.peerId,
      address,
      topics: this.getSubscribedTopics(),
      timestamp: Date.now(),
    });

    return this.publish(this.discoveryTopic, beacon);
  }

  /**
   * Get all connected peers.
   */
  public getConnectedPeers(): string[] {
    const connected: string[] = [];
    for (const [id, peer] of this.peers.entries()) {
      if (peer.connected) {
        connected.push(id);
      }
    }
    return connected;
  }

  /**
   * Get all peers known to be subscribed to a topic.
   */
  public getTopicPeers(topic: string): string[] {
    const peers: string[] = [];
    for (const [id, peer] of this.peers.entries()) {
      if (peer.connected && peer.subscribedTopics.has(topic)) {
        peers.push(id);
      }
    }
    return peers;
  }

  /**
   * Get peers in the active Gossipsub mesh for a given topic.
   */
  public getMeshPeers(topic: string): string[] {
    const mesh = this.topicMesh.get(topic);
    return mesh ? Array.from(mesh) : [];
  }

  /**
   * Periodic heartbeat maintenance loop:
   * 1. Balance topic mesh degrees (graft if < D_low, prune if > D_high).
   * 2. Evict expired seen message IDs.
   * 3. Send peer discovery beacon.
   */
  public heartbeat(): void {
    const now = Date.now();

    // 1. Maintain topic meshes
    for (const [topic, mesh] of this.topicMesh.entries()) {
      if (!this.topicSubscriptions.has(topic)) continue;

      const eligiblePeers = this.getTopicPeers(topic).filter((p) => !mesh.has(p));

      // Need more peers?
      if (mesh.size < this.config.meshDegreeLow && eligiblePeers.length > 0) {
        const needed = Math.min(this.config.meshDegree - mesh.size, eligiblePeers.length);
        for (let i = 0; i < needed; i++) {
          mesh.add(eligiblePeers[i]);
        }
        this.emit('mesh:update', topic, Array.from(mesh));
      }

      // Too many peers? Prune excess down to meshDegree
      if (mesh.size > this.config.meshDegreeHigh) {
        const excess = mesh.size - this.config.meshDegree;
        const meshArray = Array.from(mesh);
        for (let i = 0; i < excess; i++) {
          mesh.delete(meshArray[i]);
        }
        this.emit('mesh:update', topic, Array.from(mesh));
      }
    }

    // 2. Clean seen message cache
    for (const [id, timestamp] of this.seenMessageIds.entries()) {
      if (now - timestamp > this.config.seenCacheTtlMs) {
        this.seenMessageIds.delete(id);
      }
    }

    // 3. Check for stale peers
    for (const [peerId, peer] of this.peers.entries()) {
      if (peer.connected && now - peer.lastSeen > this.config.peerTtlMs) {
        peer.connected = false;
        this.emit('peer:disconnected', peerId, 'timeout');
      }
    }
  }

  // --- Internal Helpers ---

  private handleDiscoveryMessage(msg: GossipsubMessage): void {
    if (msg.from === this.peerId) return;

    try {
      const data = JSON.parse(msg.data);
      if (data.peerId && data.peerId !== this.peerId) {
        const discoveredPeerId = data.peerId;
        const address = data.address;
        const topics = Array.isArray(data.topics) ? data.topics : [];

        this.handlePeerConnected(discoveredPeerId, address);
        this.updatePeerSubscriptions(discoveredPeerId, topics);
      }
    } catch {
      // Ignore unparseable discovery beacons
    }
  }

  private graftTopicMesh(topic: string): void {
    let mesh = this.topicMesh.get(topic);
    if (!mesh) {
      mesh = new Set<string>();
      this.topicMesh.set(topic, mesh);
    }

    const candidates = this.getTopicPeers(topic).filter((p) => !mesh!.has(p));
    const targetToAdd = Math.min(this.config.meshDegree - mesh.size, candidates.length);

    for (let i = 0; i < targetToAdd; i++) {
      mesh.add(candidates[i]);
    }

    this.emit('mesh:update', topic, Array.from(mesh));
  }

  private pruneTopicMesh(topic: string): void {
    const mesh = this.topicMesh.get(topic);
    if (mesh) {
      mesh.clear();
      this.emit('mesh:update', topic, []);
    }
  }

  private recordPeerActivity(peerId: string): void {
    const peer = this.peers.get(peerId);
    if (peer) {
      peer.lastSeen = Date.now();
      peer.connected = true;
    }
  }

  private computeMessageId(
    topic: string,
    from: string,
    seqno: number,
    data: string,
  ): string {
    return crypto
      .createHash('sha256')
      .update(`${topic}:${from}:${seqno}:${data}`)
      .digest('hex');
  }
}
