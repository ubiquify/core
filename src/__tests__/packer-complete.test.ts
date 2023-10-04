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
import { Block, Version } from '../types'
import { graphPackerFactory } from '../graph-packer'

describe('pack complete history and versions', function () {
    test('versioned item list completex', async () => {
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
        const itemList: ItemList = itemListFactory(versionStore, graphStore)
        const tx = itemList.tx()
        await tx.start()
        for (let i = 0; i < 100; i++) {
            const itemValue: ItemValue = new Map<number, any>()
            itemValue.set(KeyTypes.ID, i)
            itemValue.set(KeyTypes.NAME, `item ${i}`)
            itemValue.set(KeyTypes.CONTENT, largeArray(1024 * 256, i))
            await tx.push(itemValue)
        }
        const { root: original } = await tx.commit({ comment: 'original' })
        const tx1 = itemList.tx()
        await tx1.start()
        for (let i = 100; i < 200; i++) {
            const itemValue: ItemValue = new Map<number, any>()
            itemValue.set(KeyTypes.ID, i)
            itemValue.set(KeyTypes.NAME, `item user1 ${i}`)
            itemValue.set(KeyTypes.CONTENT, largeArray(1024 * 128, i * 13))
            await tx1.push(itemValue)
        }
        const { root: first } = await tx1.commit({ comment: 'first user' })
        const tx2 = itemList.tx()
        await tx2.start()
        for (let i = 200; i < 300; i++) {
            const itemValue: ItemValue = new Map<number, any>()
            itemValue.set(KeyTypes.ID, i)
            itemValue.set(KeyTypes.NAME, `item user2 ${i}`)
            itemValue.set(KeyTypes.CONTENT, largeArray(1024 * 128, i * 17))
            await tx2.push(itemValue)
        }
        const { root: second } = await tx2.commit({ comment: 'second user' })
        const { packGraphComplete, restoreGraphComplete } =
            graphPackerFactory(linkCodec)

        // pack complete graph history incl. all versions
        const completeBundle: Block = await packGraphComplete(
            versionStore.versionStoreRoot(),
            blockStore,
            chunk,
            valueCodec
        )

        const transientStore = memoryBlockStoreFactory()

        // restore complete graph history incl. all versions
        const { versionStoreRoot, versionRoots, blocks } =
            await restoreGraphComplete(completeBundle.bytes, transientStore)

        assert.strictEqual(
            versionStoreRoot.toString(),
            versionStore.versionStoreRoot().toString()
        )
        assert.strictEqual(versionRoots.length, 3)
        assert.strictEqual(versionRoots[0].toString(), second.toString())
        assert.strictEqual(versionRoots[1].toString(), first.toString())
        assert.strictEqual(versionRoots[2].toString(), original.toString())

        const versionStore1: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: transientStore,
            versionRoot: versionRoots[0],
            storeRoot: versionStore.versionStoreRoot(),
        })

        const graphStore1 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: transientStore,
        })

        assert.strictEqual(
            versionStore1.versionStoreRoot().toString(),
            versionStore.versionStoreRoot().toString()
        )
        assert.strictEqual(versionStore1.id(), versionStore.id())
        assert.strictEqual(
            versionStore1.currentRoot().toString(),
            versionStore.currentRoot().toString()
        )

        const versions1: Version[] = versionStore1.log()
        const versions: Version[] = versionStore.log()

        assert.strictEqual(versions1.length, versions.length)
        assert.strictEqual(
            versions1[0].root.toString(),
            versions[0].root.toString()
        )
        assert.strictEqual(
            versions1[1].root.toString(),
            versions[1].root.toString()
        )
        assert.strictEqual(
            versions1[2].root.toString(),
            versions[2].root.toString()
        )

        const itemList1: ItemList = itemListFactory(versionStore1, graphStore1)
        const len = await itemList1.length()
        assert.strictEqual(len, 300)
        const firstItem = await itemList1.get(0)
        assert.strictEqual(firstItem.value.get(KeyTypes.ID), 0)
        assert.strictEqual(firstItem.value.get(KeyTypes.NAME), 'item 0')
        assert.strictEqual(
            firstItem.value.get(KeyTypes.CONTENT).length,
            1024 * 256
        )
        const lastItem = await itemList1.get(299)
        assert.strictEqual(lastItem.value.get(KeyTypes.ID), 299)
        assert.strictEqual(lastItem.value.get(KeyTypes.NAME), 'item user2 299')
        assert.strictEqual(
            lastItem.value.get(KeyTypes.CONTENT).length,
            1024 * 128
        )
    })
})

const largeArray = (size: number, value: number): Uint8Array => {
    const array = new Uint8Array(size)
    for (let i = 0; i < size; i++) {
        array[i] = value
    }
    return array
}
