import { EventEmitter } from 'events';
import crypto from 'crypto';
import logger from '../logger';

/**
 * Libp2p Gossipsub configuration options.
 */
export interface GossipSubConfig {
  /** Target degree for the gossip mesh (default: 6) */
  D?: number;
  /** Lower bound for mesh degree before GRAFTing additional peers (default: 4) */
  D_low?: number;
  /** Upper bound for mesh degree before PRUNing excess peers (default: 12) */
  D_high?: number;
  /** Gossip emission factor for peers outside the mesh (default: 3) */
  D_lazy?: number;
  /** Gossipsub heartbeat interval in milliseconds (default: 1000) */
  heartbeatIntervalMs?: number;
  /** Time-to-live for fanout state on unsubscribed publish topics in ms (default: 60000) */
  fanoutTTLMs?: number;
  /** Discovery topic for peer presence announcements */
  discoveryTopic?: string;
  /** Unique peer identifier for local node */
  peerId?: string;
  /** Listening network addresses for peer discovery (e.g. ws://127.0.0.1:8080) */
  listenAddresses?: string[];
  /** Optional public key for cryptographic peer identity (e.g. Stellar Ed25519) */
  publicKey?: string;
  /** Whether to automatically connect to peers when discovered (default: true) */
  autoConnect?: boolean;
}

/**
 * Information maintained about connected or discovered peers.
 */
export interface PeerInfo {
  peerId: string;
  addresses: string[];
  publicKey?: string;
  connectedAt: number;
  lastSeen: number;
  topics: Set<string>;
  state: 'connected' | 'disconnected' | 'connecting';
}

/**
 * Message envelope broadcasted across Gossipsub topics.
 */
export interface PubSubMessage {
  msgId: string;
  topic: string;
  data: string;
  from: string;
  seqno: number;
  timestamp: number;
  signature?: string;
}

/**
 * Periodic announcement payload sent over the discovery topic.
 */
export interface PeerDiscoveryAnnouncement {
  type: 'peer_discovery_announcement';
  peerId: string;
  addresses: string[];
  publicKey?: string;
  topics: string[];
  timestamp: number;
}

/**
 * Control message types for Gossipsub mesh maintenance.
 */
export type GossipControlMessage =
  | { type: 'GRAFT'; topic: string; peerId: string }
  | { type: 'PRUNE'; topic: string; peerId: string; backoff?: number }
  | { type: 'IHAVE'; topic: string; msgIds: string[] }
  | { type: 'IWANT'; msgIds: string[] };

/**
 * Libp2p-inspired Gossipsub & Peer Discovery Implementation.
 * Provides mesh-based topic pubsub, automatic peer discovery, and connection lifecycle management.
 */
export class GossipSubService extends EventEmitter {
  public readonly config: Required<GossipSubConfig>;
  public readonly peerId: string;

  /** Connected / known peers: peerId -> PeerInfo */
  private peers: Map<string, PeerInfo> = new Map();

  /** Mesh peers per topic: topic -> Set<peerId> */
  private mesh: Map<string, Set<string>> = new Map();

  /** Fanout peers for topics we publish to without subscribing: topic -> Set<peerId> */
  private fanout: Map<string, Set<string>> = new Map();

  /** Last publish timestamp per fanout topic: topic -> timestamp */
  private fanoutLastPub: Map<string, number> = new Map();

  /** Local topic subscriptions and their handlers: topic -> Set<callback> */
  private subscriptions: Map<string, Set<(msg: PubSubMessage) => void>> = new Map();

  /** Message cache (mcache) for deduplication and relay */
  private seenMessageIds: Set<string> = new Set();
  private mcache: Map<string, PubSubMessage> = new Map();
  private mcacheHistory: string[] = [];
  private readonly maxCacheSize = 1000;

  /** Sequence counter for outgoing messages */
  private seqno = 0;

  /** Heartbeat timer */
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private isRunning = false;

  constructor(options: GossipSubConfig = {}) {
    super();

    this.peerId = options.peerId || `peer-${crypto.randomBytes(8).toString('hex')}`;

    this.config = {
      D: options.D ?? 6,
      D_low: options.D_low ?? 4,
      D_high: options.D_high ?? 12,
      D_lazy: options.D_lazy ?? 3,
      heartbeatIntervalMs: options.heartbeatIntervalMs ?? 1000,
      fanoutTTLMs: options.fanoutTTLMs ?? 60000,
      discoveryTopic: options.discoveryTopic ?? 'web3-msg/peer-discovery/v1.0.0',
      peerId: this.peerId,
      listenAddresses: options.listenAddresses ?? [],
      publicKey: options.publicKey ?? '',
      autoConnect: options.autoConnect ?? true,
    };

    // Auto-subscribe to the default discovery topic
    this.subscribe(this.config.discoveryTopic, (msg) => {
      this.handleDiscoveryMessage(msg);
    });

    this.startHeartbeat();
  }

  // ---------------------------------------------------------------------------
  // Lifecycle Management
  // ---------------------------------------------------------------------------

  public startHeartbeat(): void {
    if (this.isRunning) return;
    this.isRunning = true;

    this.heartbeatTimer = setInterval(() => {
      this.heartbeat();
    }, this.config.heartbeatIntervalMs);

    // Initial announcement
    this.broadcastDiscoveryAnnouncement();
  }

  public stop(): void {
    this.isRunning = false;
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    this.removeAllListeners();
  }

  // ---------------------------------------------------------------------------
  // Topic Subscription Logic
  // ---------------------------------------------------------------------------

  /**
   * Subscribes to a pubsub topic. Initializes the mesh for this topic.
   */
  public subscribe(topic: string, handler?: (msg: PubSubMessage) => void): void {
    if (!this.subscriptions.has(topic)) {
      this.subscriptions.set(topic, new Set());
      this.mesh.set(topic, new Set());

      // If we had fanout peers for this topic, transfer them to the mesh
      if (this.fanout.has(topic)) {
        const fanoutPeers = this.fanout.get(topic)!;
        for (const peerId of fanoutPeers) {
          this.mesh.get(topic)!.add(peerId);
        }
        this.fanout.delete(topic);
        this.fanoutLastPub.delete(topic);
      }

      // Graft initial peers up to D
      this.replenishMesh(topic);
      this.emit('subscription:change', { type: 'subscribe', topic });
    }

    if (handler) {
      this.subscriptions.get(topic)!.add(handler);
    }
  }

  /**
   * Unsubscribes from a pubsub topic. Prunes all peers from the topic mesh.
   */
  public unsubscribe(topic: string): void {
    if (!this.subscriptions.has(topic)) return;

    this.subscriptions.delete(topic);

    // Prune mesh peers
    const meshPeers = this.mesh.get(topic);
    if (meshPeers) {
      for (const peerId of meshPeers) {
        this.emit('control:prune', { type: 'PRUNE', topic, peerId });
      }
      this.mesh.delete(topic);
    }

    this.emit('subscription:change', { type: 'unsubscribe', topic });
  }

  /**
   * Returns list of currently subscribed topics.
   */
  public getSubscriptions(): string[] {
    return Array.from(this.subscriptions.keys());
  }

  /**
   * Checks whether the node is subscribed to a topic.
   */
  public isSubscribed(topic: string): boolean {
    return this.subscriptions.has(topic);
  }

  // ---------------------------------------------------------------------------
  // Message Publishing & Ingestion
  // ---------------------------------------------------------------------------

  /**
   * Publishes a message to a topic. Disseminates to mesh or fanout peers.
   */
  public async publish(topic: string, data: string | object): Promise<PubSubMessage> {
    const payloadStr = typeof data === 'string' ? data : JSON.stringify(data);
    this.seqno += 1;

    const msgId = crypto
      .createHash('sha256')
      .update(`${this.peerId}:${this.seqno}:${topic}:${payloadStr}`)
      .digest('hex');

    const message: PubSubMessage = {
      msgId,
      topic,
      data: payloadStr,
      from: this.peerId,
      seqno: this.seqno,
      timestamp: Date.now(),
    };

    // Cache local message
    this.cacheMessage(message);

    // Forward to peers
    this.disseminate(message);

    // Deliver locally if subscribed
    this.deliverLocal(message);

    return message;
  }

  /**
   * Disseminates a message to the target topic's mesh or fanout peers.
   */
  private disseminate(message: PubSubMessage): void {
    const { topic } = message;
    let targetPeers: Set<string> | undefined;

    if (this.isSubscribed(topic)) {
      targetPeers = this.mesh.get(topic);
    } else {
      // Use fanout
      if (!this.fanout.has(topic)) {
        this.fanout.set(topic, new Set());
        // Populate fanout peers
        const available = this.getConnectedPeerIds();
        for (const pid of available.slice(0, this.config.D)) {
          this.fanout.get(topic)!.add(pid);
        }
      }
      this.fanoutLastPub.set(topic, Date.now());
      targetPeers = this.fanout.get(topic);
    }

    if (targetPeers) {
      for (const peerId of targetPeers) {
        this.emit('message:send', { peerId, message });
      }
    }
  }

  /**
   * Ingests an incoming raw or deserialized pubsub message.
   * Performs deduplication, caching, and forwarding according to Gossipsub rules.
   */
  public handleIncomingMessage(raw: string | PubSubMessage): PubSubMessage | null {
    let message: PubSubMessage;

    try {
      message = typeof raw === 'string' ? JSON.parse(raw) : raw;
    } catch {
      return null;
    }

    if (!message || !message.msgId || !message.topic || !message.from) {
      return null;
    }

    // Ignore self messages
    if (message.from === this.peerId) {
      return null;
    }

    // Deduplication check
    if (this.seenMessageIds.has(message.msgId)) {
      return null;
    }

    this.cacheMessage(message);

    // Deliver to local subscribers if subscribed
    if (this.isSubscribed(message.topic)) {
      this.deliverLocal(message);
    }

    // Gossipsub forwarding: forward to mesh peers for this topic (excluding sender)
    const meshPeers = this.mesh.get(message.topic);
    if (meshPeers) {
      for (const peerId of meshPeers) {
        if (peerId !== message.from) {
          this.emit('message:send', { peerId, message });
        }
      }
    }

    return message;
  }

  private deliverLocal(message: PubSubMessage): void {
    this.emit('message', message);
    this.emit(`message:${message.topic}`, message);

    const handlers = this.subscriptions.get(message.topic);
    if (handlers) {
      for (const handler of handlers) {
        try {
          handler(message);
        } catch (err) {
          logger.error({ err, topic: message.topic }, 'Error in pubsub message handler');
        }
      }
    }
  }

  private cacheMessage(message: PubSubMessage): void {
    this.seenMessageIds.add(message.msgId);
    this.mcache.set(message.msgId, message);
    this.mcacheHistory.push(message.msgId);

    // Prune oldest if cache capacity exceeded
    if (this.mcacheHistory.length > this.maxCacheSize) {
      const oldestId = this.mcacheHistory.shift()!;
      this.seenMessageIds.delete(oldestId);
      this.mcache.delete(oldestId);
    }
  }

  // ---------------------------------------------------------------------------
  // Peer Connection & Disconnection Event Handling
  // ---------------------------------------------------------------------------

  /**
   * Invoked when a peer establishes a network connection with this node.
   */
  public handlePeerConnected(peerId: string, addresses: string[] = [], publicKey?: string): void {
    if (peerId === this.peerId) return;

    let peer = this.peers.get(peerId);
    const now = Date.now();

    if (!peer) {
      peer = {
        peerId,
        addresses: [...addresses],
        publicKey,
        connectedAt: now,
        lastSeen: now,
        topics: new Set(),
        state: 'connected',
      };
      this.peers.set(peerId, peer);
    } else {
      peer.state = 'connected';
      peer.lastSeen = now;
      for (const addr of addresses) {
        if (!peer.addresses.includes(addr)) {
          peer.addresses.push(addr);
        }
      }
      if (publicKey) peer.publicKey = publicKey;
    }

    // Check all subscribed topics to see if mesh needs replenishment
    for (const topic of this.subscriptions.keys()) {
      const topicMesh = this.mesh.get(topic);
      if (topicMesh && topicMesh.size < this.config.D) {
        this.graft(topic, peerId);
      }
    }

    this.emit('peer:connect', peerId, peer);
  }

  /**
   * Invoked when a peer connection is terminated.
   * Removes peer from all topic meshes and fanouts.
   */
  public handlePeerDisconnected(peerId: string, reason?: string): void {
    const peer = this.peers.get(peerId);
    if (peer) {
      peer.state = 'disconnected';
      peer.lastSeen = Date.now();
    }

    // Clean from meshes
    for (const [topic, meshPeers] of this.mesh.entries()) {
      if (meshPeers.has(peerId)) {
        meshPeers.delete(peerId);
        this.emit('control:prune', { type: 'PRUNE', topic, peerId });
        // Immediately try to replenish mesh
        this.replenishMesh(topic);
      }
    }

    // Clean from fanouts
    for (const fanoutPeers of this.fanout.values()) {
      fanoutPeers.delete(peerId);
    }

    this.emit('peer:disconnect', peerId, reason);
  }

  // ---------------------------------------------------------------------------
  // Gossipsub Mesh Maintenance (GRAFT & PRUNE)
  // ---------------------------------------------------------------------------

  /**
   * Adds a peer to the topic mesh.
   */
  public graft(topic: string, peerId: string): void {
    if (!this.mesh.has(topic)) {
      this.mesh.set(topic, new Set());
    }

    const topicMesh = this.mesh.get(topic)!;
    if (!topicMesh.has(peerId)) {
      topicMesh.add(peerId);
      this.emit('control:graft', { type: 'GRAFT', topic, peerId });
    }
  }

  /**
   * Removes a peer from the topic mesh.
   */
  public prune(topic: string, peerId: string): void {
    const topicMesh = this.mesh.get(topic);
    if (topicMesh && topicMesh.has(peerId)) {
      topicMesh.delete(peerId);
      this.emit('control:prune', { type: 'PRUNE', topic, peerId });
    }
  }

  /**
   * Replenishes the mesh for a topic if degree is below D_low.
   */
  private replenishMesh(topic: string): void {
    const topicMesh = this.mesh.get(topic);
    if (!topicMesh) return;

    if (topicMesh.size < this.config.D_low) {
      const available = this.getConnectedPeerIds().filter((pid) => !topicMesh.has(pid));
      const needed = this.config.D - topicMesh.size;

      // Pick randomly up to needed
      for (const candidate of available.slice(0, needed)) {
        this.graft(topic, candidate);
      }
    }
  }

  /**
   * Prunes excess peers from topic mesh if degree exceeds D_high.
   */
  private balanceMesh(topic: string): void {
    const topicMesh = this.mesh.get(topic);
    if (!topicMesh) return;

    if (topicMesh.size > this.config.D_high) {
      const excess = topicMesh.size - this.config.D;
      const peersToPrune = Array.from(topicMesh).slice(0, excess);
      for (const peerId of peersToPrune) {
        this.prune(topic, peerId);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Gossipsub Heartbeat & Discovery Mechanics
  // ---------------------------------------------------------------------------

  /**
   * Periodic Gossipsub heartbeat function:
   * 1. Maintains mesh degree between [D_low, D_high]
   * 2. Evicts expired fanout topics (> fanoutTTLMs)
   * 3. Publishes peer discovery presence announcements
   */
  public heartbeat(): void {
    const now = Date.now();

    // 1. Mesh maintenance
    for (const topic of this.subscriptions.keys()) {
      this.replenishMesh(topic);
      this.balanceMesh(topic);
    }

    // 2. Fanout eviction
    for (const [topic, lastPub] of this.fanoutLastPub.entries()) {
      if (now - lastPub > this.config.fanoutTTLMs) {
        this.fanout.delete(topic);
        this.fanoutLastPub.delete(topic);
      }
    }

    // 3. Periodic discovery announcement
    this.broadcastDiscoveryAnnouncement();

    this.emit('gossip:heartbeat');
  }

  /**
   * Broadcasts a discovery announcement to inform peers of address and topics.
   */
  public broadcastDiscoveryAnnouncement(): void {
    if (this.config.listenAddresses.length === 0) return;

    const announcement: PeerDiscoveryAnnouncement = {
      type: 'peer_discovery_announcement',
      peerId: this.peerId,
      addresses: this.config.listenAddresses,
      publicKey: this.config.publicKey || undefined,
      topics: this.getSubscriptions(),
      timestamp: Date.now(),
    };

    this.publish(this.config.discoveryTopic, announcement).catch(() => {});
  }

  /**
   * Handles incoming peer discovery announcements.
   * If a new peer is discovered and autoConnect is true, triggers connection!
   */
  private handleDiscoveryMessage(msg: PubSubMessage): void {
    try {
      const announcement = JSON.parse(msg.data) as PeerDiscoveryAnnouncement;
      if (
        announcement &&
        announcement.type === 'peer_discovery_announcement' &&
        announcement.peerId &&
        announcement.peerId !== this.peerId
      ) {
        // Register or update peer info
        let peer = this.peers.get(announcement.peerId);
        if (!peer) {
          peer = {
            peerId: announcement.peerId,
            addresses: announcement.addresses || [],
            publicKey: announcement.publicKey,
            connectedAt: 0,
            lastSeen: Date.now(),
            topics: new Set(announcement.topics || []),
            state: 'disconnected',
          };
          this.peers.set(announcement.peerId, peer);
        } else {
          peer.lastSeen = Date.now();
          if (announcement.addresses) {
            for (const addr of announcement.addresses) {
              if (!peer.addresses.includes(addr)) {
                peer.addresses.push(addr);
              }
            }
          }
          if (announcement.topics) {
            for (const t of announcement.topics) {
              peer.topics.add(t);
            }
          }
        }

        this.emit('peer:discovered', announcement);

        // Auto-connect if enabled and peer not currently connected
        if (this.config.autoConnect && peer.state === 'disconnected') {
          for (const address of peer.addresses) {
            this.emit('peer:autoconnect', { peerId: peer.peerId, address });
          }
        }
      }
    } catch {
      // Ignored malformed announcement
    }
  }

  // ---------------------------------------------------------------------------
  // Query & Inspection Getters
  // ---------------------------------------------------------------------------

  public getConnectedPeerIds(): string[] {
    const connected: string[] = [];
    for (const [peerId, peer] of this.peers.entries()) {
      if (peer.state === 'connected') {
        connected.push(peerId);
      }
    }
    return connected;
  }

  public getMeshPeers(topic: string): string[] {
    const peers = this.mesh.get(topic);
    return peers ? Array.from(peers) : [];
  }

  public getFanoutPeers(topic: string): string[] {
    const peers = this.fanout.get(topic);
    return peers ? Array.from(peers) : [];
  }

  public getPeerInfo(peerId: string): PeerInfo | undefined {
    return this.peers.get(peerId);
  }

  public getAllPeers(): PeerInfo[] {
    return Array.from(this.peers.values());
  }
}
