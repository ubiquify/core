import { Json } from 'aws-sdk/clients/robomaker'

interface Part {
    [x: string]: any
    status: Status
    offset: Offset
    type?: Type
}

interface Vertex extends Part {
    nextEdge?: EdgeRef
    nextProp?: PropRef
    nextIndex?: IndexRef
}

interface Edge extends Part {
    source: VertexRef
    target: VertexRef
    sourcePrev?: EdgeRef
    sourceNext?: EdgeRef
    targetPrev?: EdgeRef
    targetNext?: EdgeRef
    nextProp?: PropRef
}

interface Prop extends Part {
    key: KeyType
    value: PropValue
    nextProp?: PropRef
}

interface PropRecord extends Part {
    key: KeyType
    valueRef: ValueRef
    nextProp?: PropRef
}

interface Index extends Part {
    key: KeyType
    value: Link
    nextIndex?: IndexRef
}

// 1 byte
enum Status {
    UNKNOWN = 0x0,
    CREATED = 0x1,
    UPDATED = 0x4,
    DELETED = 0x8,
}

type Offset = number & {}

type Ref = Offset & {}

type VertexRef = Ref & {}

type EdgeRef = Ref & {}

type PropRef = Ref & {}

type IndexRef = Ref & {}

type ValueRef = {
    propRef: Ref
    ref: Ref
    length: number
}

enum ElemType {
    VERTEX = 0x0,
    EDGE = 0x1,
    PROP = 0x2,
}

type ExtRef = {
    extRoot: string
    elemType: ElemType
    elemOffset: Offset
}

type Type = number & {}

type VertexType = Type & {}

type EdgeType = Type & {}

type PropType = Type & {}

type KeyType = Type & {}

type IndexType = Type & {}

type ShortString<MaxBytes> = string & {
    shield: never
}

type Link = {
    bytes: Uint8Array
}

type PropValue = any

type IndexedValue = {
    value: any
    ref: Ref
}

type Block = {
    cid: Link
    bytes: Uint8Array
}

type RootStruct = {
    vertexRoot: Link
    vertexOffset: number
    edgeRoot: Link
    edgeOffset: number
    propRoot: Link
    propOffset: number
    valueRoot: Link
    valueOffset: number
    indexRoot: Link
    indexOffset: number
}

type RootIndex = RootStruct & {
    vertexIndex: BlockIndex
    edgeIndex: BlockIndex
    propIndex: BlockIndex
    valueIndex: BlockIndex
    indexIndex: BlockIndex
}

type BlockIndex = {
    indexStruct: {
        startOffsets: Map<number, any>
        indexSize: number
        byteArraySize: number
    }
    indexBuffer: Uint8Array
}

type ContentDiff = {
    added: Link[]
    removed: Link[]
}

type Comment = any

type Tag = any

type Version = {
    root: Link
    parent?: Link
    mergeParent?: Link
    details: VersionDetails
}

type VersionDetails = {
    comment?: Comment
    tags?: Tag[]
    author?: string
    email?: string
    publicKey?: JsonWebKey
    signature?: string
    timestamp: number
    merge?: {
        parent: VersionDetails
        mergeParent: VersionDetails
    }
}

// function isShortString<MaxBytes>(text: string, maxBytes: MaxBytes): text is ShortString<MaxBytes> {
//     return pack(text).byteLength <= maxBytes
// }

// function shortString<MaxBytes extends number>(text: unknown, max: MaxBytes): ShortString<MaxBytes> {
//     if (typeof text !== 'string') throw new Error("text not string")
//     if (!isShortString(text, max)) throw new Error("text too large")
//     return text
// }

export {
    Vertex,
    Edge,
    Prop,
    PropRecord,
    Part,
    Index,
    Offset,
    Ref,
    VertexRef,
    EdgeRef,
    PropRef,
    IndexRef,
    ValueRef,
    ElemType,
    ExtRef,
    Status,
    Type,
    VertexType,
    EdgeType,
    PropType,
    KeyType,
    IndexType,
    PropValue,
    IndexedValue,
    ShortString,
    Link,
    Block,
    RootStruct,
    RootIndex,
    BlockIndex,
    ContentDiff,
    Version,
    VersionDetails,
    Comment,
    Tag,
}
