import { WebSocket, WebSocketServer } from 'ws';
import crypto from 'crypto';
import { Keypair } from '@stellar/stellar-sdk';
import { signMessage, verifyMessage, serializeMessage, deserializeMessage } from './core/signing';
import { GossipSubService, GossipSubConfig, PubSubMessage } from './core/gossipsub';
import logger from './logger';

export class MessagingNode {
  private server: WebSocketServer;
  private peers: Set<WebSocket> = new Set();
  private peerSockets: Map<string, WebSocket> = new Map();
  private socketPeerIds: Map<WebSocket, string> = new Map();
  private connectedAddresses: Set<string> = new Set();
  private seenHashes = new Set<string>();

  /** Gossipsub PubSub & Peer Discovery Service */
  public readonly pubsub: GossipSubService;

  /**
   * @param port           Port to listen on for incoming peer connections.
   * @param peerAddresses  WebSocket addresses of peers to connect to on startup.
   * @param keypair        Stellar Keypair used to sign outgoing messages and authenticate
   *                       the node. When provided, all received messages must carry a valid
   *                       Stellar signature or they are silently dropped.
   * @param gossipConfig   Optional Gossipsub / Pubsub configuration.
   */
  constructor(
    private port: number,
    peerAddresses: string[] = [],
    private keypair?: Keypair,
    gossipConfig?: Partial<GossipSubConfig>,
  ) {
    this.server = new WebSocketServer({ port });

    // Initialize Gossipsub peer discovery
    const selfAddress = `ws://127.0.0.1:${port}`;
    this.connectedAddresses.add(selfAddress);

    this.pubsub = new GossipSubService({
      listenAddresses: [selfAddress],
      publicKey: keypair ? keypair.publicKey() : undefined,
      ...gossipConfig,
    });

    // Wire Gossipsub message forwarding to connected WebSocket peer
    this.pubsub.on('message:send', ({ peerId, message }: { peerId: string; message: PubSubMessage }) => {
      const ws = this.peerSockets.get(peerId);
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(message));
      }
    });

    // Wire Gossipsub automatic peer discovery: when peer announced, connect automatically!
    this.pubsub.on('peer:autoconnect', ({ address }: { peerId: string; address: string }) => {
      if (!this.connectedAddresses.has(address)) {
        this.connectToPeer(address);
      }
    });

    this.server.on('connection', (ws) => {
      this.setupPeer(ws);
    });

    for (const address of peerAddresses) {
      this.connectToPeer(address);
    }
  }

  public connectToPeer(address: string): WebSocket {
    this.connectedAddresses.add(address);
    const ws = new WebSocket(address);

    ws.on('open', () => {
      this.setupPeer(ws, address);
    });
    ws.on('error', (err) => {
      logger.error({ address, err }, 'Failed to connect to peer');
    });

    return ws;
  }

  private setupPeer(ws: WebSocket, remoteAddress?: string) {
    const peerId = `ws-peer-${crypto.randomBytes(4).toString('hex')}`;
    this.peerSockets.set(peerId, ws);
    this.socketPeerIds.set(ws, peerId);
    this.peers.add(ws);

    // Register with Gossipsub service
    const addresses = remoteAddress ? [remoteAddress] : [];
    this.pubsub.handlePeerConnected(peerId, addresses);

    ws.on('message', (data) => {
      this.processMessage(data.toString(), ws);
    });

    const cleanup = () => {
      const pid = this.socketPeerIds.get(ws);
      if (pid) {
        this.pubsub.handlePeerDisconnected(pid);
        this.peerSockets.delete(pid);
        this.socketPeerIds.delete(ws);
      }
      this.peers.delete(ws);
    };

    ws.on('close', cleanup);
    ws.on('error', cleanup);
  }

  private processMessage(raw: string, senderWs?: WebSocket) {
    // 1. Check if message is a Gossipsub PubSubMessage
    try {
      const parsed = JSON.parse(raw);
      if (parsed && parsed.msgId && parsed.topic && parsed.from) {
        // Associate sender's peerId if known
        if (senderWs) {
          const currentPid = this.socketPeerIds.get(senderWs);
          if (currentPid && currentPid !== parsed.from) {
            this.peerSockets.delete(currentPid);
            this.peerSockets.set(parsed.from, senderWs);
            this.socketPeerIds.set(senderWs, parsed.from);
            this.pubsub.handlePeerConnected(parsed.from, [], parsed.publicKey);
          }
        }

        const delivered = this.pubsub.handleIncomingMessage(parsed);
        if (delivered) return;
      }
    } catch {
      // Not a JSON pubsub message — proceed to standard message processing
    }

    // 2. Standard MessagingNode signature verification
    if (this.keypair) {
      const envelope = deserializeMessage(raw);
      if (!envelope || !verifyMessage(envelope)) {
        // Invalid or unsigned message — drop silently
        return;
      }
    }

    const hash = crypto.createHash('sha256').update(raw).digest('hex');
    if (this.seenHashes.has(hash)) return;

    this.seenHashes.add(hash);
    this.broadcastInternal(raw);
  }

  /**
   * Broadcast a plain string payload across all connected peers.
   */
  async broadcast(msg: string): Promise<boolean> {
    const wire = this.keypair
      ? serializeMessage(signMessage(msg, this.keypair))
      : msg;

    const hash = crypto.createHash('sha256').update(wire).digest('hex');
    this.seenHashes.add(hash);
    this.broadcastInternal(wire);
    return true;
  }

  /**
   * Subscribe to a pubsub topic.
   */
  public subscribe(topic: string, handler?: (msg: PubSubMessage) => void): void {
    this.pubsub.subscribe(topic, handler);
  }

  /**
   * Unsubscribe from a pubsub topic.
   */
  public unsubscribe(topic: string): void {
    this.pubsub.unsubscribe(topic);
  }

  /**
   * Publish a message to a pubsub topic.
   */
  public async publish(topic: string, data: string | object): Promise<PubSubMessage> {
    return this.pubsub.publish(topic, data);
  }

  private broadcastInternal(msg: string) {
    for (const peer of this.peers) {
      if (peer.readyState === WebSocket.OPEN) {
        peer.send(msg);
      }
    }
  }

  public getConnectedPeerCount(): number {
    return this.peers.size;
  }

  close() {
    this.pubsub.stop();
    this.server.close();
    for (const peer of this.peers) {
      peer.close();
    }
  }
}
