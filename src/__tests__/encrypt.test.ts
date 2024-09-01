import {
    Secrets,
    Secret,
    Cipher,
    cipherFactory,
    secretsFactory,
} from '../encrypt'
import * as assert from 'assert'
import crypto from 'crypto'
import { BlockStore, memoryBlockStoreFactory } from '../block-store'
import { VersionStore, versionStoreFactory } from '../version-store'
import { chunkerFactory } from '../chunking'
import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import {
    LinkCodec,
    linkCodecFactory,
    ValueCodec,
    valueCodecFactory,
} from '../codecs'
import { graphStore } from '../graph-store'
import { Graph } from '../graph'
import { OFFSET_INCREMENTS } from '../serde'
import { navigateVertices, PathElemType, RequestBuilder } from '../navigate'
import { Prop } from '../types'

const { subtle } = crypto.webcrypto
const { chunk } = chunkerFactory(1024, compute_chunks)
enum ObjectTypes {
    TWEET = 1,
}
enum RlshpTypes {
    COMMENT_TO = 1,
}
enum PropTypes {
    COMMENT = 1,
}
enum KeyTypes {
    JSON = 1,
}

describe('Encryption essentials', () => {
    test('Encryption artifacts', async () => {
        const secrets = secretsFactory({ subtle })
        const secret = await secrets.generateSecret()
        const cipher = cipherFactory({ subtle, secret })
        const original = new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05])
        const encrypted = await cipher.encrypt(original)
        const jwk = await secrets.exportSecret(secret)
        const persistedKey = JSON.stringify(jwk)
        const jwk2 = JSON.parse(persistedKey)
        const secret2 = await secrets.importSecret(jwk2)
        const cipher2 = cipherFactory({
            subtle,
            secret: secret2,
        })
        const decrypted = await cipher2.decrypt(encrypted)
        assert.deepStrictEqual(original, decrypted)
    })

    test('Block encoding and encryption', async () => {
        // encryption
        const secrets: Secrets = secretsFactory({ subtle })
        const secret: Secret = await secrets.generateSecret()
        const cipher: Cipher = cipherFactory({ subtle, secret })

        const memoryStore: BlockStore = memoryBlockStoreFactory()
        const linkCodec: LinkCodec = linkCodecFactory()
        const valueCodec: ValueCodec = valueCodecFactory(cipher)
        const story: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: memoryStore,
        })
        const store = graphStore({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: memoryStore,
        })
        const graph = new Graph(story, store)
        const tx = graph.tx()
        await tx.start()
        const v1 = tx.addVertex(ObjectTypes.TWEET)
        const v2 = tx.addVertex(ObjectTypes.TWEET)
        const v3 = tx.addVertex(ObjectTypes.TWEET)
        await tx.addEdge(v1, v2, RlshpTypes.COMMENT_TO)
        await tx.addEdge(v1, v3, RlshpTypes.COMMENT_TO)
        await tx.addVertexProp(
            v2,
            KeyTypes.JSON,
            { hello: 'v2' },
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v2,
            KeyTypes.JSON,
            { hello: 'v3' },
            PropTypes.COMMENT
        )
        const { root: sharedRoot } = await tx.commit({})

        // --- share the secret ---
        const jwk = await secrets.exportSecret(secret)
        const persistedSecret = JSON.stringify(jwk)

        // --- other user reads the secret ---
        const jwk2 = JSON.parse(persistedSecret)
        const secret2 = await secrets.importSecret(jwk2)
        const cipher2 = cipherFactory({
            subtle,
            secret: secret2,
        })

        // create new graph using the shared secret, shared root and the encrypted block store
        const linkCodec2: LinkCodec = linkCodecFactory()
        const valueCodec2: ValueCodec = valueCodecFactory(cipher2)
        const story2: VersionStore = await versionStoreFactory({
            versionRoot: sharedRoot,
            chunk,
            linkCodec: linkCodec2,
            valueCodec: valueCodec2,
            blockStore: memoryStore,
        })
        const store2 = graphStore({
            chunk,
            linkCodec: linkCodec2,
            valueCodec: valueCodec2,
            blockStore: memoryStore,
        })
        const graph2 = new Graph(story2, store2)
        const request2 = new RequestBuilder()
            .add(PathElemType.VERTEX)
            .add(PathElemType.EDGE)
            .add(PathElemType.VERTEX)
            .extract(KeyTypes.JSON)
            .get()

        const vr: any[] = []
        for await (const result of navigateVertices(graph2, [0], request2)) {
            const p: Prop = result
            vr.push(p.value)
        }

        assert.deepStrictEqual(vr, [{ hello: 'v2' }, { hello: 'v3' }])

        // --- for encryption unaware users, property extraction should fail ---
        const linkCodec3: LinkCodec = linkCodecFactory()
        const valueCodec3: ValueCodec = valueCodecFactory() // no cipher
        const story3: VersionStore = await versionStoreFactory({
            versionRoot: sharedRoot,
            chunk,
            linkCodec: linkCodec3,
            valueCodec: valueCodec3,
            blockStore: memoryStore,
        })
        const store3 = graphStore({
            chunk,
            linkCodec: linkCodec3,
            valueCodec: valueCodec3,
            blockStore: memoryStore,
        })
        const graph3 = new Graph(story3, store3)
        const request3 = new RequestBuilder()
            .add(PathElemType.VERTEX)
            .add(PathElemType.EDGE)
            .add(PathElemType.VERTEX)
            .extract(KeyTypes.JSON)
            .get()
        let error = null
        try {
            for await (const result of navigateVertices(
                graph3,
                [0],
                request3
            )) {
            }
        } catch (e) {
            error = e
        }
        assert.notStrictEqual(error, null)
        assert.strictEqual(error.message, 'Decoding error')
    })
})
