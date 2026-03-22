import { mkdtempSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import path from 'path';

import {
  createConversation,
  createInvite,
  createMessage,
  decryptMessage,
  defaultTTL,
  deriveConversationKeys,
  deserializeEnvelope,
  generateIdentity,
  serializeEnvelope,
} from '@corpollc/qntm';
import type {
  Conversation,
  DropboxSubscription,
  DropboxSubscriptionHandlers,
  Identity,
  SubscriptionMessage,
} from '@corpollc/qntm';
import { describe, expect, test, vi } from 'vitest';

vi.mock('../logger.js', () => ({
  logger: {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  },
}));

import { readConversationCursor, writeConversationCursor } from '../qntm-state.js';
import { toHex } from '../qntm.js';
import type { NewMessage, RegisteredGroup } from '../types.js';
import { createQntmChannelFactory } from './qntm.js';

function createConversationFixture(type: 'direct' | 'group' = 'direct') {
  const inviter = generateIdentity();
  const invite = createInvite(inviter, type);
  const conversation = createConversation(invite, deriveConversationKeys(invite));
  return {
    inviter,
    invite,
    conversation,
    conversationId: toHex(conversation.id),
  };
}

function createIdentityDirFixture(params?: {
  identity?: Identity;
  conversations?: Array<
    ReturnType<typeof createConversationFixture> & {
      name?: string;
    }
  >;
}) {
  const identity = params?.identity ?? generateIdentity();
  const dir = mkdtempSync(path.join(tmpdir(), 'nanoclaw-qntm-'));
  writeFileSync(
    path.join(dir, 'identity.json'),
    `${JSON.stringify(
      {
        private_key: toHex(identity.privateKey),
        public_key: toHex(identity.publicKey),
        key_id: toHex(identity.keyID),
      },
      null,
      2,
    )}\n`,
    'utf-8',
  );
  writeFileSync(
    path.join(dir, 'conversations.json'),
    `${JSON.stringify(
      (params?.conversations ?? []).map((conversation) => ({
        id: conversation.conversationId,
        name: conversation.name,
        type: conversation.conversation.type,
        keys: {
          root: toHex(conversation.conversation.keys.root),
          aead_key: toHex(conversation.conversation.keys.aeadKey),
          nonce_key: toHex(conversation.conversation.keys.nonceKey),
        },
        participants: conversation.conversation.participants.map((participant) =>
          toHex(participant),
        ),
        created_at: conversation.conversation.createdAt.toISOString(),
        current_epoch: conversation.conversation.currentEpoch,
      })),
      null,
      2,
    )}\n`,
    'utf-8',
  );
  return {
    dir,
    identity,
    cleanup: () => rmSync(dir, { recursive: true, force: true }),
  };
}

function createStateDirFixture() {
  const dir = mkdtempSync(path.join(tmpdir(), 'nanoclaw-qntm-state-'));
  return {
    dir,
    cleanup: () => rmSync(dir, { recursive: true, force: true }),
  };
}

function createEnvelopeFixture(params: {
  sender: Identity;
  conversation: Conversation;
  text: string;
  bodyType?: string;
}) {
  const envelope = createMessage(
    params.sender,
    params.conversation,
    params.bodyType ?? 'text',
    new TextEncoder().encode(params.text),
    undefined,
    defaultTTL(),
  );
  return {
    envelope,
    serialized: serializeEnvelope(envelope),
    messageId: toHex(envelope.msg_id),
  };
}

function createMockClient() {
  const subscriptions = new Map<string, DropboxSubscriptionHandlers>();
  const postMessageCalls: Array<{ conversationId: Uint8Array; envelope: Uint8Array }> = [];
  const subscribeCalls: Array<{ conversationId: string; fromSequence: number }> = [];

  const client = {
    postMessage: async (conversationId: Uint8Array, envelope: Uint8Array) => {
      postMessageCalls.push({ conversationId, envelope });
      return 1;
    },
    subscribeMessages: (
      conversationId: Uint8Array,
      fromSequence: number,
      handlers: DropboxSubscriptionHandlers,
    ): DropboxSubscription => {
      const conversationKey = toHex(conversationId);
      subscriptions.set(conversationKey, handlers);
      subscribeCalls.push({
        conversationId: conversationKey,
        fromSequence,
      });
      return {
        close: () => undefined,
        closed: Promise.resolve(),
      };
    },
  };

  return {
    client,
    postMessageCalls,
    subscribeCalls,
    async emit(conversationId: string, message: SubscriptionMessage) {
      const handlers = subscriptions.get(conversationId);
      if (!handlers) {
        throw new Error(`no subscription for ${conversationId}`);
      }
      await handlers.onMessage(message);
    },
    subscriptionCount() {
      return subscriptions.size;
    },
  };
}

function createOpts(registeredGroups: Record<string, RegisteredGroup>) {
  const messages: Array<{ jid: string; message: NewMessage }> = [];
  const metadata: Array<{
    jid: string;
    timestamp: string;
    name?: string;
    channel?: string;
    isGroup?: boolean;
  }> = [];
  const opts = {
    onMessage: (jid: string, message: NewMessage) => {
      messages.push({ jid, message });
    },
    onChatMetadata: (
      jid: string,
      timestamp: string,
      name?: string,
      channel?: string,
      isGroup?: boolean,
    ) => {
      metadata.push({ jid, timestamp, name, channel, isGroup });
    },
    registeredGroups: () => registeredGroups,
  };
  return {
    opts,
    messages,
    metadata,
  };
}

describe('QntmChannel', () => {
  test('factory returns null when QNTM_IDENTITY_DIR is missing', () => {
    const factory = createQntmChannelFactory({
      env: {},
      envFileValues: {},
    });
    const { opts } = createOpts({});
    expect(factory(opts)).toBeNull();
  });

  test('connect subscribes to registered qntm groups and delivers inbound text', async () => {
    const conversation = createConversationFixture('group');
    const identityDir = createIdentityDirFixture({
      conversations: [{ ...conversation, name: 'Ops Room' }],
    });
    const stateDir = createStateDirFixture();
    const mock = createMockClient();
    const { opts, messages, metadata } = createOpts({
      [`qntm:${conversation.conversationId}`]: {
        name: 'Ops Room',
        folder: 'qntm_ops',
        trigger: '@Andy',
        added_at: new Date().toISOString(),
      },
    });

    const factory = createQntmChannelFactory({
      env: {
        QNTM_IDENTITY_DIR: identityDir.dir,
        QNTM_RELAY_URL: 'https://relay.example.test',
      },
      envFileValues: {},
      stateDir: stateDir.dir,
      createClient: () => mock.client,
    });
    const channel = factory(opts);
    expect(channel).not.toBeNull();

    await channel!.connect();
    expect(mock.subscriptionCount()).toBe(1);

    const sender = generateIdentity();
    const inbound = createEnvelopeFixture({
      sender,
      conversation: conversation.conversation,
      text: 'hello from qntm',
    });
    await mock.emit(conversation.conversationId, {
      seq: 7,
      envelope: inbound.serialized,
    });

    expect(messages).toEqual([
      expect.objectContaining({
        jid: `qntm:${conversation.conversationId}`,
        message: expect.objectContaining({
          id: inbound.messageId,
          chat_jid: `qntm:${conversation.conversationId}`,
          content: 'hello from qntm',
        }),
      }),
    ]);
    expect(metadata).toEqual([
      expect.objectContaining({
        jid: `qntm:${conversation.conversationId}`,
        name: 'Ops Room',
        channel: 'qntm',
        isGroup: true,
      }),
    ]);
    expect(
      readConversationCursor({
        conversationId: conversation.conversationId,
        stateDir: stateDir.dir,
      }),
    ).toBe(7);

    identityDir.cleanup();
    stateDir.cleanup();
  });

  test('sendMessage encrypts text to the requested conversation', async () => {
    const conversation = createConversationFixture('direct');
    const identityDir = createIdentityDirFixture({
      conversations: [{ ...conversation, name: 'Alice' }],
    });
    const stateDir = createStateDirFixture();
    const mock = createMockClient();
    const { opts } = createOpts({});

    const factory = createQntmChannelFactory({
      env: {
        QNTM_IDENTITY_DIR: identityDir.dir,
        QNTM_RELAY_URL: 'https://relay.example.test',
      },
      envFileValues: {},
      stateDir: stateDir.dir,
      createClient: () => mock.client,
    });
    const channel = factory(opts);
    expect(channel).not.toBeNull();

    await channel!.sendMessage(`qntm:${conversation.conversationId}`, 'ship it');

    expect(mock.postMessageCalls).toHaveLength(1);
    expect(mock.postMessageCalls[0]?.conversationId).toEqual(
      conversation.conversation.id,
    );
    const envelope = deserializeEnvelope(mock.postMessageCalls[0]!.envelope);
    const decrypted = decryptMessage(envelope, conversation.conversation);
    expect(new TextDecoder().decode(decrypted.inner.body)).toBe('ship it');

    identityDir.cleanup();
    stateDir.cleanup();
  });

  test('connect resumes from the persisted cursor for each conversation', async () => {
    const conversation = createConversationFixture('group');
    const identityDir = createIdentityDirFixture({
      conversations: [{ ...conversation, name: 'Ops Room' }],
    });
    const stateDir = createStateDirFixture();
    writeConversationCursor({
      conversationId: conversation.conversationId,
      sequence: 6,
      stateDir: stateDir.dir,
      updatedAt: 1,
    });
    const mock = createMockClient();
    const { opts } = createOpts({
      [`qntm:${conversation.conversationId}`]: {
        name: 'Ops Room',
        folder: 'qntm_ops',
        trigger: '@Andy',
        added_at: new Date().toISOString(),
      },
    });

    const factory = createQntmChannelFactory({
      env: {
        QNTM_IDENTITY_DIR: identityDir.dir,
      },
      envFileValues: {},
      stateDir: stateDir.dir,
      createClient: () => mock.client,
    });
    const channel = factory(opts);
    expect(channel).not.toBeNull();

    await channel!.connect();

    expect(mock.subscribeCalls).toEqual([
      {
        conversationId: conversation.conversationId,
        fromSequence: 6,
      },
    ]);

    identityDir.cleanup();
    stateDir.cleanup();
  });

  test('self-authored qntm messages are ignored', async () => {
    const conversation = createConversationFixture('direct');
    const identityDir = createIdentityDirFixture({
      conversations: [{ ...conversation, name: 'Alice' }],
    });
    const stateDir = createStateDirFixture();
    const mock = createMockClient();
    const { opts, messages } = createOpts({
      [`qntm:${conversation.conversationId}`]: {
        name: 'Alice',
        folder: 'qntm_alice',
        trigger: '@Andy',
        added_at: new Date().toISOString(),
      },
    });

    const factory = createQntmChannelFactory({
      env: {
        QNTM_IDENTITY_DIR: identityDir.dir,
      },
      envFileValues: {},
      stateDir: stateDir.dir,
      createClient: () => mock.client,
    });
    const channel = factory(opts);
    expect(channel).not.toBeNull();
    await channel!.connect();

    const selfAuthored = createEnvelopeFixture({
      sender: identityDir.identity,
      conversation: conversation.conversation,
      text: 'ignore me',
    });
    await mock.emit(conversation.conversationId, {
      seq: 3,
      envelope: selfAuthored.serialized,
    });

    expect(messages).toEqual([]);
    expect(
      readConversationCursor({
        conversationId: conversation.conversationId,
        stateDir: stateDir.dir,
      }),
    ).toBe(3);

    identityDir.cleanup();
    stateDir.cleanup();
  });

  test('non-text bodies are surfaced with a readable type prefix', async () => {
    const conversation = createConversationFixture('group');
    const identityDir = createIdentityDirFixture({
      conversations: [{ ...conversation, name: 'Ops Room' }],
    });
    const stateDir = createStateDirFixture();
    const mock = createMockClient();
    const { opts, messages } = createOpts({
      [`qntm:${conversation.conversationId}`]: {
        name: 'Ops Room',
        folder: 'qntm_ops',
        trigger: '@Andy',
        added_at: new Date().toISOString(),
      },
    });

    const factory = createQntmChannelFactory({
      env: {
        QNTM_IDENTITY_DIR: identityDir.dir,
      },
      envFileValues: {},
      stateDir: stateDir.dir,
      createClient: () => mock.client,
    });
    const channel = factory(opts);
    expect(channel).not.toBeNull();
    await channel!.connect();

    const sender = generateIdentity();
    const inbound = createEnvelopeFixture({
      sender,
      conversation: conversation.conversation,
      text: '{"request":"approve"}',
      bodyType: 'gate.request',
    });
    await mock.emit(conversation.conversationId, {
      seq: 9,
      envelope: inbound.serialized,
    });

    expect(messages[0]?.message.content).toBe(
      '[gate.request] {"request":"approve"}',
    );

    identityDir.cleanup();
    stateDir.cleanup();
  });
});
