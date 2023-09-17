import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from '../chunking'
import { GraphStore, graphStoreFactory } from '../graph-store'
import { Graph } from '../graph'
import {
    BlockStore,
    MemoryBlockStore,
    memoryBlockStoreFactory,
} from '../block-store'
import {
    LinkCodec,
    linkCodecFactory,
    ValueCodec,
    valueCodecFactory,
} from '../codecs'
import * as assert from 'assert'
import { navigateVertices, PathElemType, RequestBuilder } from '../navigate'
import { eq } from '../ops'
import {
    Link,
    Offset,
    Part,
    Prop,
    Comment,
    Tag,
    Block,
    Version,
} from '../types'
import { merge, MergePolicyEnum } from '../merge'
import { VersionStore, versionStoreFactory } from '../version-store'
import { link } from 'fs'
import { graphPackerFactory } from '../graph-packer'
import { version } from 'uuid'

/**
 * Some proto-schema
 */

enum ObjectTypes {
    FOLDER = 1,
    FILE = 2,
}

enum RlshpTypes {
    CONTAINS = 1,
}

enum PropTypes {
    META = 1,
    DATA = 2,
}

enum KeyTypes {
    NAME = 1,
    CONTENT = 2,
}

const { chunk } = chunkerFactory(512, compute_chunks)
const linkCodec: LinkCodec = linkCodecFactory()
const valueCodec: ValueCodec = valueCodecFactory()

describe('Version store merge details', function () {
    test('Merge parents are preserved after restore', async () => {
        const blockStore1: MemoryBlockStore = memoryBlockStoreFactory()
        const blockStore2: MemoryBlockStore = memoryBlockStoreFactory()
        /**
         * Build original data set
         */
        const versionStore: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore1,
        })

        const graphStore: GraphStore = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore1,
        })

        const graph = new Graph(versionStore, graphStore)

        const tx = graph.tx()

        await tx.start()

        const v1 = tx.addVertex(ObjectTypes.FOLDER)
        const v2 = tx.addVertex(ObjectTypes.FOLDER)
        const v3 = tx.addVertex(ObjectTypes.FILE)

        const e1 = await tx.addEdge(v1, v2, RlshpTypes.CONTAINS)
        const e2 = await tx.addEdge(v1, v3, RlshpTypes.CONTAINS)

        await tx.addVertexProp(v1, KeyTypes.NAME, 'root-folder', PropTypes.META)
        await tx.addVertexProp(
            v2,
            KeyTypes.NAME,
            'nested-folder',
            PropTypes.META
        )
        await tx.addVertexProp(v3, KeyTypes.NAME, 'nested-file', PropTypes.META)
        await tx.addVertexProp(
            v2,
            KeyTypes.CONTENT,
            'hello world from v2',
            PropTypes.DATA
        )
        await tx.addVertexProp(
            v3,
            KeyTypes.CONTENT,
            'hello world from v3',
            PropTypes.DATA
        )

        const { root: original } = await tx.commit({})

        // transfer all blocks from initial commit
        await blockStore1.push(blockStore2)

        const originalVersionStoreId = versionStore.id()

        /**
         * Revise original, first user
         */
        const versionStore1: VersionStore = await versionStoreFactory({
            versionRoot: original,
            storeRoot: versionStore.versionStoreRoot(),
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore1,
        })

        const graphStore1 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore1,
        })
        const g1 = new Graph(versionStore1, graphStore1)

        const tx1 = g1.tx()
        await tx1.start()
        const v10 = await tx1.getVertex(0)
        const v11 = tx1.addVertex(ObjectTypes.FILE)
        const e11 = await tx1.addEdge(v10, v11, RlshpTypes.CONTAINS)
        await tx1.addVertexProp(
            v11,
            KeyTypes.NAME,
            'nested-file-user-1',
            PropTypes.META
        )
        await tx1.addVertexProp(
            v11,
            KeyTypes.CONTENT,
            'hello world from v11',
            PropTypes.DATA
        )

        const { root: first } = await tx1.commit({
            comment: 'first commit',
            tags: ['first'],
        })

        assert.equal(versionStore1.id(), originalVersionStoreId)

        const log1 = versionStore1.log()

        /**
         * Revise original, second user
         */
        const versionStore2: VersionStore = await versionStoreFactory({
            versionRoot: original,
            storeRoot: versionStore.versionStoreRoot(),
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore2,
        })

        const graphStore2 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore2,
        })
        const g2 = new Graph(versionStore2, graphStore2)

        const tx2 = g2.tx()
        await tx2.start()
        const v20 = await tx2.getVertex(0)
        const v21 = tx2.addVertex(ObjectTypes.FILE)
        const e21 = await tx2.addEdge(v20, v21, RlshpTypes.CONTAINS)
        await tx2.addVertexProp(
            v21,
            KeyTypes.NAME,
            'nested-file-user-2',
            PropTypes.META
        )
        await tx2.addVertexProp(
            v21,
            KeyTypes.CONTENT,
            'hello world from v21',
            PropTypes.DATA
        )

        const { root: second, blocks: secondBlocks } = await tx2.commit({
            comment: 'second commit',
            tags: ['second'],
        })

        assert.equal(versionStore2.id(), originalVersionStoreId)

        const log2 = versionStore2.log()

        /**
         * Create a bundle w/ blocks present in versionStore2/blockStore2 and missing in versionStore1/blocksStore1
         */
        const bundle: Block = await versionStore1.packMissingBlocks(
            versionStore2,
            blockStore2
        )

        const { restoreRandomBlocks } = graphPackerFactory(linkCodec)

        /**
         * Restore blocks from the bundle into the blockStore1
         */
        await restoreRandomBlocks(bundle.bytes, blockStore1)

        /**
         * Merge versionStore2 into versionStore1
         */
        const {
            root: mergedRoot,
            index: mergedIndex,
            blocks: mergedBlocks,
        } = await versionStore1.mergeVersions(versionStore2)

        const versions1 = versionStore1.log()

        const versionStoreRoot = versionStore1.versionStoreRoot()
        const versionRoot = versionStore1.currentRoot()

        expect(mergedRoot.toString()).toEqual(versionRoot.toString())

        // imply version root latest
        const versionStoreNew1: VersionStore = await versionStoreFactory({
            storeRoot: versionStoreRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore1,
        })
        const versionsNew1 = versionStoreNew1.log()

        expect(versions1).toEqual(versionsNew1)
        expect(versionStore1.id()).toEqual(versionStoreNew1.id())
        expect(versionStore1.versionStoreRoot().toString()).toEqual(
            versionStoreNew1.versionStoreRoot().toString()
        )
        expect(versionStore1.currentRoot().toString()).toEqual(
            versionStoreNew1.currentRoot().toString()
        )

        // specify versionRoot
        const versionStoreNew2: VersionStore = await versionStoreFactory({
            storeRoot: versionStoreRoot,
            versionRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore1,
        })
        const versionsNew2 = versionStoreNew2.log()
        expect(versions1).toEqual(versionsNew2)
        expect(versionStore1.id()).toEqual(versionStoreNew2.id())
        expect(versionStore1.versionStoreRoot().toString()).toEqual(
            versionStoreNew2.versionStoreRoot().toString()
        )
        expect(versionStore1.currentRoot().toString()).toEqual(
            versionStoreNew2.currentRoot().toString()
        )

        const mergeVersion = versions1[0]
        console.log(
            'mergeVersionsDetails',
            JSON.stringify(mergeVersion.details, null, 2)
        )
        const mergeVersionsDetails = mergeVersion.details.merge
        const mergeVersionParent = mergeVersionsDetails.parent
        const mergeVersionMergeParent = mergeVersionsDetails.mergeParent
        expect(mergeVersionParent).toEqual(versionStore1.log()[1].details)
        expect(mergeVersionMergeParent).toEqual(versionStore2.log()[0].details)
    })
})
