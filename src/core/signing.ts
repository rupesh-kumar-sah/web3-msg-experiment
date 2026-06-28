import { Keypair } from '@stellar/stellar-sdk';
import { SignedMessage } from './types';

/**
 * Sign a string payload with the given Stellar Keypair.
 * The payload is encoded to UTF-8 bytes before signing.
 */
export function signMessage(payload: string, keypair: Keypair): SignedMessage {
  const payloadBytes = Buffer.from(payload, 'utf8');
  const signatureBytes = keypair.sign(payloadBytes);
  return {
    payload,
    senderPublicKey: keypair.publicKey(),
    signature: Buffer.from(signatureBytes).toString('hex'),
  };
}

/**
 * Verify a SignedMessage using the embedded senderPublicKey.
 * Returns true if the signature is valid, false otherwise.
 */
export function verifyMessage(message: SignedMessage): boolean {
  try {
    const { payload, senderPublicKey, signature } = message;
    const payloadBytes = Buffer.from(payload, 'utf8');
    const signatureBytes = Buffer.from(signature, 'hex');
    const keypair = Keypair.fromPublicKey(senderPublicKey);
    return keypair.verify(payloadBytes, signatureBytes);
  } catch {
    return false;
  }
}

/**
 * Serialize a SignedMessage to a JSON string for transmission.
 */
export function serializeMessage(message: SignedMessage): string {
  return JSON.stringify(message);
}

/**
 * Verify that a payload has been signed by at least `threshold` of the given
 * `quorumPublicKeys`. Each key is checked independently; duplicate keys count
 * only once. Returns true when the quorum threshold is reached.
 */
export function verifyQuorum(
  payload: string,
  signatures: Array<{ publicKey: string; signature: string }>,
  quorumPublicKeys: string[],
  threshold: number,
): boolean {
  if (threshold <= 0 || quorumPublicKeys.length === 0) return false;
  const payloadBytes = Buffer.from(payload, 'utf8');
  const verified = new Set<string>();
  for (const { publicKey, signature } of signatures) {
    if (verified.has(publicKey)) continue;
    if (!quorumPublicKeys.includes(publicKey)) continue;
    try {
      const kp = Keypair.fromPublicKey(publicKey);
      if (kp.verify(payloadBytes, Buffer.from(signature, 'hex'))) {
        verified.add(publicKey);
        if (verified.size >= threshold) return true;
      }
    } catch {
      // invalid key or signature bytes — skip
    }
  }
  return verified.size >= threshold;
}

/**
 * Deserialize a JSON string into a SignedMessage.
 * Returns null if parsing fails or required fields are missing.
 */
export function deserializeMessage(raw: string): SignedMessage | null {
  try {
    const parsed = JSON.parse(raw);
    if (
      typeof parsed.payload === 'string' &&
      typeof parsed.senderPublicKey === 'string' &&
      typeof parsed.signature === 'string'
    ) {
      return parsed as SignedMessage;
    }
    return null;
  } catch {
    return null;
  }
}
