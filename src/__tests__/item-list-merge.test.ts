import {
    linkCodecFactory,
    LinkCodec,
    ValueCodec,
    valueCodecFactory,
} from '../codecs'
import { graphStoreFactory } from '../graph-store'
import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from '../chunking'
import {
    BlockStore,
    MemoryBlockStore,
    memoryBlockStoreFactory,
} from '../block-store'
import * as assert from 'assert'
import { VersionStore, versionStoreFactory } from '../version-store'
import {
    Item,
    ItemList,
    itemListFactory,
    ItemListTransaction,
    ItemRef,
    ItemValue,
    mergeItemLists,
    readonlyItemList,
} from '../item-list'
import { merge } from '../merge'

describe('revise and merge item list', function () {
    test('simple', async () => {
        const { chunk } = chunkerFactory(512, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const valueCodec: ValueCodec = valueCodecFactory()
        const blockStore: BlockStore = memoryBlockStoreFactory()
        const versionStore: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })
        const graphStore = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        enum KeyTypes {
            NAME = 1,
        }
        const itemListOrig: ItemList = itemListFactory(versionStore, graphStore)
        const tx = itemListOrig.tx()
        await tx.start()
        await tx.push(new Map([[KeyTypes.NAME, 'item 0']]))
        await tx.push(new Map([[KeyTypes.NAME, 'item 1']]))
        await tx.push(new Map([[KeyTypes.NAME, 'item 2']]))
        const { root: original } = await tx.commit({})

        /**
         * Revise original, first user
         */

        const graphStore1 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        const itemList1: ItemList = itemListFactory(versionStore, graphStore1)
        const tx1 = itemList1.tx()
        await tx1.start()
        await tx1.push(new Map([[KeyTypes.NAME, 'item user1']]))
        const { root: first } = await tx1.commit({})

        /**
         * Revise original, second user
         */
        versionStore.checkout(original)

        const graphStore2 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        const itemList2: ItemList = itemListFactory(versionStore, graphStore2)
        const tx2 = itemList2.tx()
        await tx2.start()
        await tx2.push(new Map([[KeyTypes.NAME, 'item user2']]))
        const { root: second } = await tx2.commit({})

        const {
            root: mergeRoot,
            index: mergeIndex,
            blocks: mergeBlocks,
        } = await mergeItemLists(
            {
                baseRoot: original,
                baseStore: blockStore,
                currentRoot: first,
                currentStore: blockStore,
                otherRoot: second,
                otherStore: blockStore,
            },
            chunk,
            linkCodec,
            valueCodec
        )

        const versionStoreNew: VersionStore = await versionStoreFactory({
            versionRoot: mergeRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        const graphStoreNew = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        const itemListMerged: ItemList = itemListFactory(
            versionStoreNew,
            graphStoreNew
        )

        const len = await itemListMerged.length()

        assert.strictEqual(5, len)

        const item0 = await itemListMerged.get(0)
        assert.strictEqual('item 0', item0.value.get(KeyTypes.NAME))

        const item1 = await itemListMerged.get(1)
        assert.strictEqual('item 1', item1.value.get(KeyTypes.NAME))

        const item2 = await itemListMerged.get(2)
        assert.strictEqual('item 2', item2.value.get(KeyTypes.NAME))

        const item3 = await itemListMerged.get(3)
        assert.strictEqual('item user2', item3.value.get(KeyTypes.NAME))

        const item4 = await itemListMerged.get(4)
        assert.strictEqual('item user1', item4.value.get(KeyTypes.NAME))
    })

    test('history properly maintained across merges', async () => {
        const { chunk } = chunkerFactory(512, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const valueCodec: ValueCodec = valueCodecFactory()
        const blockStore: MemoryBlockStore = memoryBlockStoreFactory()
        const versionStore: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })
        const graphStore = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        enum KeyTypes {
            ID = 1,
            NAME = 2,
            CONTENT = 3,
        }
        const itemListOrig: ItemList = itemListFactory(versionStore, graphStore)
        const tx = itemListOrig.tx()
        await tx.start()
        for (let i = 0; i < 100; i++) {
            const itemValue: ItemValue = new Map<number, any>()
            itemValue.set(KeyTypes.ID, i)
            itemValue.set(KeyTypes.NAME, `item ${i}`)
            itemValue.set(KeyTypes.CONTENT, largeArray(1024 * 256, i))
            await tx.push(itemValue)
        }
        const { root: original } = await tx.commit({ comment: 'original' })

        const blockStore1: MemoryBlockStore = memoryBlockStoreFactory()
        const blockStore2: MemoryBlockStore = memoryBlockStoreFactory()
        await blockStore.push(blockStore1)
        await blockStore.push(blockStore2)
        const originalVersionStoreId = versionStore.id()
        const originalVersionStoreRoot = versionStore.versionStoreRoot()
        const originalVersionRoot = versionStore.currentRoot()

        /**
         * Revise original, first user
         */
        const versionStore1: VersionStore = await versionStoreFactory({
            versionRoot: originalVersionRoot,
            storeRoot: originalVersionStoreRoot,
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

        const itemList1: ItemList = itemListFactory(versionStore1, graphStore1)
        const tx1 = itemList1.tx()
        await tx1.start()
        // await tx1.push(new Map([[KeyTypes.NAME, 'item user1']]))
        for (let i = 100; i < 200; i++) {
            const itemValue: ItemValue = new Map<number, any>()
            itemValue.set(KeyTypes.ID, i)
            itemValue.set(KeyTypes.NAME, `item user1 ${i}`)
            itemValue.set(KeyTypes.CONTENT, largeArray(1024 * 128, i * 13))
            await tx1.push(itemValue)
        }
        const { root: first } = await tx1.commit({ comment: 'first user' })

        expect(versionStore1.includesVersion(originalVersionRoot)).toBeTruthy()
        expect(versionStore1.includesVersion(first)).toBeTruthy()

        const firstVersionStoreId = versionStore1.id()
        const firstVersionStoreRoot = versionStore1.versionStoreRoot()
        const firstVersionRoot = versionStore1.currentRoot()

        /**
         * Revise original, second user
         */
        const versionStore2: VersionStore = await versionStoreFactory({
            versionRoot: originalVersionRoot,
            storeRoot: originalVersionStoreRoot,
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

        const itemList2: ItemList = itemListFactory(versionStore2, graphStore2)
        const tx2 = itemList2.tx()
        await tx2.start()
        // await tx2.push(new Map([[KeyTypes.NAME, 'item user2']]))
        for (let i = 200; i < 300; i++) {
            const itemValue: ItemValue = new Map<number, any>()
            itemValue.set(KeyTypes.ID, i)
            itemValue.set(KeyTypes.NAME, `item user2 ${i}`)
            itemValue.set(KeyTypes.CONTENT, largeArray(1024 * 128, i * 17))
            await tx2.push(itemValue)
        }
        const { root: second } = await tx2.commit({ comment: 'second user' })

        expect(versionStore2.includesVersion(originalVersionRoot)).toBeTruthy()
        expect(versionStore2.includesVersion(second)).toBeTruthy()

        const secondVersionStoreId = versionStore2.id()
        const secondVersionStoreRoot = versionStore2.versionStoreRoot()
        const secondVersionRoot = versionStore2.currentRoot()

        const mergeStore: MemoryBlockStore = memoryBlockStoreFactory()

        // shortcut, normally packing, transferring, unpacking VersionStore and GraphVersion bits
        // packVersionStore, packGraphVersion
        await blockStore1.push(mergeStore)
        await blockStore2.push(mergeStore)

        const versionStore11: VersionStore = await versionStoreFactory({
            versionRoot: firstVersionRoot,
            storeRoot: firstVersionStoreRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: mergeStore,
        })

        const versionStore22: VersionStore = await versionStoreFactory({
            versionRoot: secondVersionRoot,
            storeRoot: secondVersionStoreRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: mergeStore,
        })

        /**
         * Merge versionStore22 into versionStore11
         */
        const {
            root: mergedRoot,
            index: mergedIndex,
            blocks: mergedBlocks,
        } = await versionStore11.mergeVersions(versionStore22)

        expect(versionStore11.includesVersion(firstVersionRoot)).toBeTruthy()
        expect(versionStore11.includesVersion(secondVersionRoot)).toBeTruthy()

        const graphStore11 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: mergeStore,
        })

        const itemListMerged: ItemList = itemListFactory(
            versionStore11,
            graphStore11
        )

        const len = await itemListMerged.length()

        assert.strictEqual(300, len)

        /**
         * With intermediate merge, merge versionStore11 into versionStore
         */
        const mergeStore2: MemoryBlockStore = memoryBlockStoreFactory()
        await blockStore1.push(mergeStore2)

        const versionStore0: VersionStore = await versionStoreFactory({
            versionRoot: originalVersionRoot,
            storeRoot: originalVersionStoreRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: mergeStore2,
        })

        expect(versionStore0.log()).toEqual(versionStore.log())

        const versionStore111: VersionStore = await versionStoreFactory({
            versionRoot: firstVersionRoot,
            storeRoot: firstVersionStoreRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: mergeStore2,
        })

        expect(versionStore111.log()).toEqual(versionStore1.log())

        const {
            root: mergedRoot2,
            index: mergedIndex2,
            blocks: mergedBlocks2,
        } = await versionStore0.mergeVersions(versionStore111)

        expect(versionStore0.includesVersion(originalVersionRoot)).toBeTruthy()
        expect(versionStore0.includesVersion(firstVersionRoot)).toBeTruthy()

        const mergedVersionStoreId = versionStore0.id()
        const mergedVersionStoreRoot = versionStore0.versionStoreRoot()
        const mergedVersionRoot = versionStore0.currentRoot()

        const graphStore111 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: mergeStore2,
        })

        const itemListMerged2: ItemList = itemListFactory(
            versionStore0,
            graphStore111
        )

        const len2 = await itemListMerged2.length()

        assert.strictEqual(200, len2)

        /**
         * Then merge versionStore22 into previous merge
         */
        const mergeStore3: MemoryBlockStore = memoryBlockStoreFactory()
        await blockStore2.push(mergeStore3)
        await mergeStore2.push(mergeStore3)

        const versionStore00: VersionStore = await versionStoreFactory({
            versionRoot: mergedVersionRoot,
            storeRoot: mergedVersionStoreRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: mergeStore3,
        })

        const versionStore222: VersionStore = await versionStoreFactory({
            versionRoot: secondVersionRoot,
            storeRoot: secondVersionStoreRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: mergeStore3,
        })

        const {
            root: mergedRoot3,
            index: mergedIndex3,
            blocks: mergedBlocks3,
        } = await versionStore00.mergeVersions(versionStore222)

        expect(versionStore00.includesVersion(originalVersionRoot)).toBeTruthy()
        expect(versionStore00.includesVersion(firstVersionRoot)).toBeTruthy()
        expect(versionStore00.includesVersion(secondVersionRoot)).toBeTruthy()
        expect(versionStore00.includesVersion(mergedRoot2)).toBeTruthy()
        expect(versionStore00.includesVersion(mergedRoot3)).toBeTruthy()

        const mergedVersionStoreId3 = versionStore00.id()
        const mergedVersionStoreRoot3 = versionStore00.versionStoreRoot()
        const mergedVersionRoot3 = versionStore00.currentRoot()

        const graphStore222 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: mergeStore3,
        })

        const itemListMerged3: ItemList = itemListFactory(
            versionStore00,
            graphStore222
        )

        const len3 = await itemListMerged3.length()

        assert.strictEqual(300, len3)

        expect(mergedVersionStoreId3).toEqual(firstVersionStoreId)
        expect(mergedVersionStoreId).toEqual(firstVersionStoreId)
        expect(mergedVersionStoreId).toEqual(secondVersionStoreId)
    })
})

const largeArray = (size: number, value: number): Uint8Array => {
    const array = new Uint8Array(size)
    for (let i = 0; i < size; i++) {
        array[i] = value
    }
    return array
}
