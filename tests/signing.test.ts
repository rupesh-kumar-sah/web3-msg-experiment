import { Keypair } from '@stellar/stellar-sdk';
import {
  signMessage,
  verifyMessage,
  serializeMessage,
  deserializeMessage,
  verifyQuorum,
} from '../src/core/signing';

describe('Stellar message signing', () => {
  let keypair: Keypair;

  beforeEach(() => {
    keypair = Keypair.random();
  });

  it('produces a SignedMessage with all required fields', () => {
    const envelope = signMessage('hello world', keypair);
    expect(envelope.payload).toBe('hello world');
    expect(envelope.senderPublicKey).toBe(keypair.publicKey());
    expect(typeof envelope.signature).toBe('string');
    expect(envelope.signature.length).toBeGreaterThan(0);
  });

  it('verifies a legitimately signed message', () => {
    const envelope = signMessage('hello world', keypair);
    expect(verifyMessage(envelope)).toBe(true);
  });

  it('rejects a message with a tampered payload', () => {
    const envelope = signMessage('original', keypair);
    const tampered = { ...envelope, payload: 'tampered' };
    expect(verifyMessage(tampered)).toBe(false);
  });

  it('rejects a message with a tampered signature', () => {
    const envelope = signMessage('hello', keypair);
    const tampered = { ...envelope, signature: 'deadbeef'.repeat(8) };
    expect(verifyMessage(tampered)).toBe(false);
  });

  it('rejects a message signed by a different keypair', () => {
    const otherKeypair = Keypair.random();
    const envelope = signMessage('hello', otherKeypair);
    // Swap the public key so it points to our keypair — signature won't match
    const spoofed = { ...envelope, senderPublicKey: keypair.publicKey() };
    expect(verifyMessage(spoofed)).toBe(false);
  });

  it('rejects a message with an invalid public key', () => {
    const envelope = signMessage('hello', keypair);
    const bad = { ...envelope, senderPublicKey: 'not-a-valid-key' };
    expect(verifyMessage(bad)).toBe(false);
  });

  describe('serialization round-trip', () => {
    it('serializes and deserializes correctly', () => {
      const envelope = signMessage('round-trip', keypair);
      const json = serializeMessage(envelope);
      const decoded = deserializeMessage(json);
      expect(decoded).toEqual(envelope);
    });

    it('deserializeMessage returns null for invalid JSON', () => {
      expect(deserializeMessage('not json')).toBeNull();
    });

    it('deserializeMessage returns null when fields are missing', () => {
      expect(deserializeMessage(JSON.stringify({ payload: 'x' }))).toBeNull();
    });
  });
});

describe('MessagingNode signing integration', () => {
  // We test the signing logic directly rather than spinning up live WebSocket
  // servers to keep the suite fast and deterministic.
  it('signed + serialized message verifies after round-trip', () => {
    const keypair = Keypair.random();
    const payload = 'peer-to-peer message';
    const envelope = signMessage(payload, keypair);
    const wire = serializeMessage(envelope);

    const decoded = deserializeMessage(wire);
    expect(decoded).not.toBeNull();
    expect(verifyMessage(decoded!)).toBe(true);
    expect(decoded!.payload).toBe(payload);
  });

  it('a message without a valid envelope is rejected', () => {
    const plainText = 'unsigned message';
    const decoded = deserializeMessage(plainText);
    // Plain text is not a JSON SignedMessage, so deserialization returns null
    expect(decoded).toBeNull();
  });
});

describe('verifyQuorum', () => {
  const payload = 'relay-payload';
  let kp1: Keypair, kp2: Keypair, kp3: Keypair;

  beforeEach(() => {
    kp1 = Keypair.random();
    kp2 = Keypair.random();
    kp3 = Keypair.random();
  });

  function sig(kp: Keypair): { publicKey: string; signature: string } {
    const bytes = Buffer.from(payload, 'utf8');
    return {
      publicKey: kp.publicKey(),
      signature: Buffer.from(kp.sign(bytes)).toString('hex'),
    };
  }

  it('passes when valid signatures meet the threshold', () => {
    const quorum = [kp1.publicKey(), kp2.publicKey(), kp3.publicKey()];
    expect(verifyQuorum(payload, [sig(kp1), sig(kp2)], quorum, 2)).toBe(true);
  });

  it('passes when all quorum members sign (threshold === quorum size)', () => {
    const quorum = [kp1.publicKey(), kp2.publicKey()];
    expect(verifyQuorum(payload, [sig(kp1), sig(kp2)], quorum, 2)).toBe(true);
  });

  it('fails when fewer valid signatures than threshold', () => {
    const quorum = [kp1.publicKey(), kp2.publicKey(), kp3.publicKey()];
    expect(verifyQuorum(payload, [sig(kp1)], quorum, 2)).toBe(false);
  });

  it('ignores signatures from keys not in the quorum', () => {
    const outsider = Keypair.random();
    const quorum = [kp1.publicKey(), kp2.publicKey()];
    // outsider signs but is not in quorum; only kp1 counts → below threshold 2
    expect(verifyQuorum(payload, [sig(kp1), sig(outsider)], quorum, 2)).toBe(false);
  });

  it('deduplicates repeated signatures from the same key', () => {
    const quorum = [kp1.publicKey(), kp2.publicKey()];
    expect(verifyQuorum(payload, [sig(kp1), sig(kp1)], quorum, 2)).toBe(false);
  });

  it('rejects a tampered signature from a quorum member', () => {
    const quorum = [kp1.publicKey(), kp2.publicKey()];
    const bad = { publicKey: kp1.publicKey(), signature: 'deadbeef'.repeat(8) };
    expect(verifyQuorum(payload, [bad, sig(kp2)], quorum, 2)).toBe(false);
  });

  it('returns false for empty signature list', () => {
    const quorum = [kp1.publicKey()];
    expect(verifyQuorum(payload, [], quorum, 1)).toBe(false);
  });

  it('returns false for zero threshold', () => {
    const quorum = [kp1.publicKey()];
    expect(verifyQuorum(payload, [sig(kp1)], quorum, 0)).toBe(false);
  });

  it('returns false for empty quorum set', () => {
    expect(verifyQuorum(payload, [sig(kp1)], [], 1)).toBe(false);
  });
});
