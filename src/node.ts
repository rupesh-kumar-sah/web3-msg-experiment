import { WebSocket, WebSocketServer } from 'ws';
import crypto from 'crypto';
import { Keypair } from '@stellar/stellar-sdk';
import { signMessage, verifyMessage, serializeMessage, deserializeMessage } from './core/signing';
import logger from './logger';

export class MessagingNode {
  private server: WebSocketServer;
  private peers: Set<WebSocket> = new Set();
  private seenHashes = new Set<string>();

  /**
   * @param port       Port to listen on for incoming peer connections.
   * @param peerAddresses  WebSocket addresses of peers to connect to on startup.
   * @param keypair    Stellar Keypair used to sign outgoing messages and authenticate
   *                   the node. When provided, all received messages must carry a valid
   *                   Stellar signature or they are silently dropped.
   */
  constructor(
    private port: number,
    peerAddresses: string[] = [],
    private keypair?: Keypair,
  ) {
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
      this.setupPeer(ws);
    });
    ws.on('error', (err) => {
      logger.error({ address, err }, 'Failed to connect to peer');
    });
  }

  private setupPeer(ws: WebSocket) {
    ws.on('message', (data) => {
      this.processMessage(data.toString());
    });
    ws.on('close', () => {
      this.peers.delete(ws);
    });
    ws.on('error', () => {
      this.peers.delete(ws);
    });
    this.peers.add(ws);
  }

  private processMessage(raw: string) {
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
    this.broadcastInternal(raw);
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

  close() {
    this.server.close();
    for (const peer of this.peers) {
      peer.close();
    }
  }
}
