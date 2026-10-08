import { WebSocket, WebSocketServer } from 'ws';
import crypto from 'crypto';
import { Keypair } from '@stellar/stellar-sdk';
import { signMessage, verifyMessage, serializeMessage, deserializeMessage } from './core/signing';
import { GossipsubRouter, GossipsubMessage, MessageHandler, GossipsubConfig } from './core/gossipsub';
import logger from './logger';

export { GossipsubRouter, GossipsubMessage, MessageHandler, GossipsubConfig } from './core/gossipsub';

export class MessagingNode {
  private server: WebSocketServer;
  private peers: Set<WebSocket> = new Set();
  private peerMap: Map<WebSocket, string> = new Map();
  private seenHashes = new Set<string>();
  public readonly gossipsub: GossipsubRouter;

  /**
   * @param port       Port to listen on for incoming peer connections.
   * @param peerAddresses  WebSocket addresses of peers to connect to on startup.
   * @param keypair    Stellar Keypair used to sign outgoing messages and authenticate
   *                   the node. When provided, all received messages must carry a valid
   *                   Stellar signature or they are silently dropped.
   * @param gossipConfig Optional Gossipsub mesh and discovery configuration.
   */
  constructor(
    private port: number,
    peerAddresses: string[] = [],
    private keypair?: Keypair,
    gossipConfig?: GossipsubConfig,
  ) {
    const peerId = keypair ? keypair.publicKey() : undefined;
    this.gossipsub = new GossipsubRouter(peerId, gossipConfig);
    this.gossipsub.start().catch((err) => {
      logger.error({ err }, 'Failed to start Gossipsub router');
    });

    this.server = new WebSocketServer({ port });
    this.server.on('connection', (ws) => {
      this.setupPeer(ws);
    });

    for (const address of peerAddresses) {
      this.connectToPeer(address);
    }
  }

  private connectToPeer(address: string) {
    const ws = new WebSocket(address);
    ws.on('open', () => {
      this.setupPeer(ws, address);
    });
    ws.on('error', (err) => {
      logger.error({ address, err }, 'Failed to connect to peer');
    });
  }

  private setupPeer(ws: WebSocket, address?: string) {
    const peerIdentifier = address || `ws-peer-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    this.peerMap.set(ws, peerIdentifier);
    this.peers.add(ws);

    this.gossipsub.handlePeerConnected(peerIdentifier, address);

    ws.on('message', (data) => {
      this.processMessage(data.toString(), ws);
    });
    ws.on('close', () => {
      const id = this.peerMap.get(ws) || peerIdentifier;
      this.gossipsub.handlePeerDisconnected(id);
      this.peerMap.delete(ws);
      this.peers.delete(ws);
    });
    ws.on('error', () => {
      const id = this.peerMap.get(ws) || peerIdentifier;
      this.gossipsub.handlePeerDisconnected(id);
      this.peerMap.delete(ws);
      this.peers.delete(ws);
    });

    // Send peer discovery beacon through gossipsub
    this.gossipsub.broadcastDiscoveryBeacon(address).catch(() => {});
  }

  private processMessage(raw: string, senderWs?: WebSocket) {
    // When a keypair is configured, every incoming message must be a valid SignedMessage.
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

    // If message is pubsub formatted, route to gossipsub router
    try {
      const parsed = JSON.parse(raw);
      if (parsed && parsed.topic && parsed.id) {
        const senderId = (senderWs && this.peerMap.get(senderWs)) || 'unknown';
        this.gossipsub.handleIncomingMessage(senderId, parsed);
      }
    } catch {
      // Non-JSON raw message handled below
    }

    this.broadcastInternal(raw);
  }

  /**
   * Subscribe to a pubsub topic.
   */
  async subscribe(topic: string, handler?: MessageHandler): Promise<void> {
    await this.gossipsub.subscribe(topic, handler);
  }

  /**
   * Unsubscribe from a pubsub topic.
   */
  async unsubscribe(topic: string, handler?: MessageHandler): Promise<void> {
    await this.gossipsub.unsubscribe(topic, handler);
  }

  /**
   * Publish a message to a pubsub topic.
   */
  async publish(topic: string, data: string): Promise<GossipsubMessage> {
    const msg = await this.gossipsub.publish(topic, data);
    const wire = JSON.stringify(msg);
    const finalWire = this.keypair
      ? serializeMessage(signMessage(wire, this.keypair))
      : wire;
    this.broadcastInternal(finalWire);
    return msg;
  }

  /**
   * Broadcast a plain string payload. When this node has a keypair the
   * payload is wrapped in a SignedMessage envelope before sending.
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

  private broadcastInternal(msg: string) {
    for (const peer of this.peers) {
      if (peer.readyState === WebSocket.OPEN) {
        peer.send(msg);
      }
    }
  }

  async close() {
    await this.gossipsub.stop();
    this.server.close();
    for (const peer of this.peers) {
      peer.close();
    }
  }
}

