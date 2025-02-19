import { Link, Block, IndexedValue } from './types'

import { CID } from 'multiformats/cid'
import * as Raw from 'multiformats/codecs/raw'
import { sha256 } from 'multiformats/hashes/sha2'
import { pack, unpack } from 'msgpackr'
import { chunkyStore } from '@dstanesc/store-chunky-bytes'
import { Cipher } from './encrypt'

const { create, readAll } = chunkyStore()

const CID_VERSION = 1
const CODEC_CODE = Raw.code

interface SearchIndex {
    create: (
        values: IndexedValue[],
        blockPut: (block: Block) => Promise<void>,
        blockGet: (cid: any) => Promise<Uint8Array>
    ) => Promise<Link>
    search: (
        link: Link,
        blockGet: (cid: any) => Promise<Uint8Array>,
        value: any
    ) => Promise<IndexedValue>
}

interface LinkCodec {
    encode: (blockBytes: Uint8Array) => Promise<Link>
    decode: (linkBytes: Uint8Array) => Link
    parseString: (encoded: string) => Link
    encodeString: (link: Link) => string
}

interface BlockCodec {
    encode: (
        json: any,
        blockPut: (block: Block) => Promise<void>
    ) => Promise<Link>
    decode: (
        link: Link,
        blockGet: (cid: any) => Promise<Uint8Array>
    ) => Promise<any>
}

interface ValueCodec {
    encode: (value: any) => Promise<Uint8Array>
    decode: (valueBytes: Uint8Array) => Promise<any>
}

const valueCodecFactory = (cipher?: Cipher): ValueCodec => {
    const encode = async (value: any): Promise<Uint8Array> => {
        return cipher
            ? await cipher.encrypt(pack(value) as Uint8Array)
            : (pack(value) as Uint8Array)
    }
    const decode = async (valueBytes: Uint8Array): Promise<any> => {
        try {
            return cipher
                ? unpack(await cipher.decrypt(valueBytes))
                : unpack(valueBytes)
        } catch (error) {
            throw new Error('Decoding error')
        }
    }
    return { encode, decode }
}

const linkCodecFactory = (): LinkCodec => {
    const encode = async (blockBytes: Uint8Array): Promise<Link> => {
        const hash = await sha256.digest(blockBytes)
        return CID.create(CID_VERSION, CODEC_CODE, hash)
    }
    const decode = (linkBytes: Uint8Array): Link => {
        return CID.decode(linkBytes)
    }
    const parseString = (encodedString: string): Link => {
        return CID.parse(encodedString)
    }
    const encodeString = (link: Link): string => {
        return link.toString()
    }
    return { encode, decode, encodeString, parseString }
}

const blockCodecFactory = (): BlockCodec => {
    const { encode: linkEncode, decode: linkDecode } = linkCodecFactory()
    const encode = async (
        json: any,
        blockPut: (block: Block) => Promise<void>
    ): Promise<Link> => {
        const bytes = pack(json)
        const link = await linkEncode(bytes)
        await blockPut({ cid: link, bytes })
        return link
    }
    const decode = async (
        link: Link,
        blockGet: (cid: any) => Promise<Uint8Array>
    ): Promise<any> => {
        const blockBytes = await blockGet(link)
        return unpack(blockBytes)
    }
    return { encode, decode }
}

const multiBlockCodecFactory = (
    chunk: (data: Uint8Array) => Uint32Array
): BlockCodec => {
    const { encode: linkEncode, decode: linkDecode } = linkCodecFactory()
    const encode = async (
        json: any,
        blockPut: (block: Block) => Promise<void>
    ): Promise<Link> => {
        const buf = pack(json)
        const { root, index, blocks } = await create({
            buf,
            chunk,
            encode: linkEncode,
        })
        for (const block of blocks) await blockPut(block)
        return root
    }
    const decode = async (
        link: Link,
        blockGet: (cid: any) => Promise<Uint8Array>
    ): Promise<any> => {
        const bytes = await readAll({
            root: link,
            decode: linkDecode,
            get: blockGet,
        })
        return unpack(bytes)
    }
    return { encode, decode }
}

export {
    LinkCodec,
    BlockCodec,
    ValueCodec,
    SearchIndex,
    valueCodecFactory,
    linkCodecFactory,
    blockCodecFactory,
    multiBlockCodecFactory,
}
