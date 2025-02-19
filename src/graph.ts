import {
    Vertex,
    Edge,
    Prop,
    Offset,
    VertexRef,
    EdgeRef,
    PropRef,
    VertexType,
    EdgeType,
    PropType,
    KeyType,
    PropValue,
    Link,
    Block,
    RootIndex,
    IndexedValue,
    Index,
    IndexRef,
    IndexType,
    Status,
    Part,
    Version,
    Comment,
    Tag,
    VersionDetails,
} from './types'

import { OFFSET_INCREMENTS } from './serde'
import { TraversalVisitor, traverseVertices } from './depth-first'
import { IndexStore } from './index-store'
import { PropPredicate } from './navigate'
import { Signer } from './trust'
import base64 from 'base64-js'

interface ElementAccessor {
    getVertex: (ref: VertexRef) => Promise<Vertex>
    getEdge: (ref: EdgeRef) => Promise<Edge>
    getProp: (ref: PropRef) => Promise<Prop>
}

async function* edgesOutgoing(
    vertex: Vertex,
    accessor: ElementAccessor
): AsyncGenerator<Edge, void, void> {
    yield* edgeSourceNext(vertex.nextEdge, accessor)
}

async function* edgeSourceNext(
    ref: EdgeRef,
    accessor: ElementAccessor
): AsyncGenerator<Edge, void, void> {
    if (ref !== undefined) {
        const edge = await accessor.getEdge(ref)
        yield edge
        yield* edgeSourceNext(edge.sourceNext, accessor)
    }
}

async function* propsNext(
    ref: PropRef,
    accessor: ElementAccessor
): AsyncGenerator<Prop, void, void> {
    if (ref !== undefined) {
        const prop = await accessor.getProp(ref)
        yield prop
        yield* propsNext(prop.nextProp, accessor)
    }
}

function incomingEdgesVisitor(
    incomingEdges: Map<VertexRef, Set<EdgeRef>>
): TraversalVisitor {
    const endEdge = async (edge: Edge) => {
        const edgeOffset = edge.offset
        const targetOffset = edge.target
        if (incomingEdges.has(targetOffset)) {
            const edgeSet = incomingEdges.get(targetOffset)
            edgeSet.add(edgeOffset)
        } else {
            const edgeSet = new Set<EdgeRef>()
            edgeSet.add(edgeOffset)
            incomingEdges.set(targetOffset, edgeSet)
        }
    }
    return { endEdge }
}

class Graph implements ElementAccessor {
    vertices: Map<number, Vertex>
    edges: Map<number, Edge>
    props: Map<number, Prop>
    indices: Map<number, Index>
    versionSet: ({
        version,
        index,
    }: {
        version: Version
        index: RootIndex
    }) => Promise<Link>
    rootGet: () => Promise<{ root: Link; index: RootIndex }>
    vertexGet: (
        { root, index }: { root: Link; index: RootIndex },
        offset: number
    ) => Promise<Vertex>
    vertexRange: (
        { root, index }: { root: Link; index: RootIndex },
        offset: number,
        count: number
    ) => Promise<Vertex[]>
    edgeGet: (
        { root, index }: { root: Link; index: RootIndex },
        offset: number
    ) => Promise<Edge>
    edgeRange: (
        { root, index }: { root: Link; index: RootIndex },
        offset: number,
        count: number
    ) => Promise<Edge[]>
    propGet: (
        { root, index }: { root: Link; index: RootIndex },
        offset: number
    ) => Promise<Prop>
    propRange: (
        { root, index }: { root: Link; index: RootIndex },
        offset: number,
        count: number
    ) => Promise<Prop[]>
    indexGet: (
        { root, index }: { root: Link; index: RootIndex },
        offset: number
    ) => Promise<Index>
    offsetsGet: ({ root, index }: { root: Link; index: RootIndex }) => Promise<{
        vertexOffset: Offset
        edgeOffset: Offset
        propOffset: Offset
        indexOffset: Offset
    }>
    verticesAll: ({
        root,
        index,
    }: {
        root: Link
        index: RootIndex
    }) => Promise<Vertex[]>
    edgesAll: ({
        root,
        index,
    }: {
        root: Link
        index: RootIndex
    }) => Promise<Edge[]>
    propsAll: ({
        root,
        index,
    }: {
        root: Link
        index: RootIndex
    }) => Promise<Prop[]>
    commit: (
        { root, index }: { root: Link; index: RootIndex },
        {
            vertices,
            edges,
            props,
        }: {
            vertices: {
                added: Map<number, Vertex>
                updated: Map<number, Vertex>
            }
            edges: { added: Map<number, Edge>; updated: Map<number, Edge> }
            props: { added: Map<number, Prop>; updated: Map<number, Prop> }
            indices: { added: Map<number, Index>; updated: Map<number, Index> }
        }
    ) => Promise<{ root: Link; index: RootIndex; blocks: Block[] }>
    packGraph: (versionRoot: Link) => Promise<Block>
    packComputed: (
        versionRoot: Link,
        vertexOffsetStart: number,
        vertexCount: number,
        graphDepth: number
    ) => Promise<Block>
    indexCreate: (values: IndexedValue[]) => Promise<Link>
    indexSearch: (link: Link, value: any) => Promise<IndexedValue>

    constructor(
        {
            versionSet,
            rootGet,
        }: {
            versionSet: ({
                version,
                index,
            }: {
                version: Version
                index: RootIndex
            }) => Promise<Link>
            rootGet: () => Promise<{ root: Link; index: RootIndex }>
        },
        {
            vertexGet,
            vertexRange,
            edgeGet,
            edgeRange,
            propGet,
            propRange,
            indexGet,
            offsetsGet,
            verticesAll,
            edgesAll,
            propsAll,
            commit,
            packGraph,
            packComputed,
        }: {
            vertexGet: (
                { root, index }: { root: Link; index: RootIndex },
                offset: number
            ) => Promise<Vertex>
            vertexRange: (
                { root, index }: { root: Link; index: RootIndex },
                offset: number,
                vertexCount: number
            ) => Promise<Vertex[]>
            edgeGet: (
                { root, index }: { root: Link; index: RootIndex },
                offset: number
            ) => Promise<Edge>
            edgeRange: (
                { root, index }: { root: Link; index: RootIndex },
                offset: number,
                edgeCount: number
            ) => Promise<Edge[]>
            propGet: (
                { root, index }: { root: Link; index: RootIndex },
                offset: number
            ) => Promise<Prop>
            propRange: (
                { root, index }: { root: Link; index: RootIndex },
                offset: number,
                propCount: number
            ) => Promise<Prop[]>
            indexGet: (
                { root, index }: { root: Link; index: RootIndex },
                offset: number
            ) => Promise<Index>
            offsetsGet: ({
                root,
                index,
            }: {
                root: Link
                index: RootIndex
            }) => Promise<{
                vertexOffset: Offset
                edgeOffset: Offset
                propOffset: Offset
                indexOffset: Offset
            }>
            verticesAll: ({
                root,
                index,
            }: {
                root: Link
                index: RootIndex
            }) => Promise<Vertex[]>
            edgesAll: ({
                root,
                index,
            }: {
                root: Link
                index: RootIndex
            }) => Promise<Edge[]>
            propsAll: ({
                root,
                index,
            }: {
                root: Link
                index: RootIndex
            }) => Promise<Prop[]>
            commit: (
                { root, index }: { root: Link; index: RootIndex },
                {
                    vertices,
                    edges,
                    props,
                }: {
                    vertices: {
                        added: Map<number, Vertex>
                        updated: Map<number, Vertex>
                    }
                    edges: {
                        added: Map<number, Edge>
                        updated: Map<number, Edge>
                    }
                    props: {
                        added: Map<number, Prop>
                        updated: Map<number, Prop>
                    }
                    indices: {
                        added: Map<number, Index>
                        updated: Map<number, Index>
                    }
                }
            ) => Promise<{ root: Link; index: RootIndex; blocks: Block[] }>
            packGraph: (versionRoot: Link) => Promise<Block>
            packComputed: (
                versionRoot: Link,
                vertexOffsetStart: number,
                vertexCount: number,
                graphDepth: number
            ) => Promise<Block>
        },
        { indexCreate, indexSearch }: IndexStore = {
            indexCreate: undefined,
            indexSearch: undefined,
        }
    ) {
        this.vertices = new Map()
        this.edges = new Map()
        this.props = new Map()
        this.indices = new Map()
        this.versionSet = versionSet
        this.rootGet = rootGet
        this.vertexGet = vertexGet
        this.vertexRange = vertexRange
        this.edgeGet = edgeGet
        this.edgeRange = edgeRange
        this.propGet = propGet
        this.propRange = propRange
        this.indexGet = indexGet
        this.offsetsGet = offsetsGet
        this.verticesAll = verticesAll
        this.edgesAll = edgesAll
        this.propsAll = propsAll
        this.commit = commit
        this.packGraph = packGraph
        this.packComputed = packComputed
        this.indexCreate = indexCreate
        this.indexSearch = indexSearch
    }

    async getVertex(ref: VertexRef): Promise<Vertex> {
        let vertex: Vertex = this.vertices.get(ref)
        if (vertex === undefined) {
            vertex = await this.vertexGet(await this.rootGet(), ref)
            this.vertices.set(ref, vertex)
        }
        return vertex
    }

    async getVertexRange(
        startRef: VertexRef,
        vertexCount: number
    ): Promise<Vertex[]> {
        const vertices: Vertex[] = await this.vertexRange(
            await this.rootGet(),
            startRef,
            vertexCount
        )
        let vertexRef = startRef
        for (let i = 0; i < vertexCount; i++) {
            this.vertices.set(vertexRef, vertices[i])
            vertexRef += OFFSET_INCREMENTS.VERTEX_INCREMENT
        }
        return vertices
    }

    async getEdge(ref: EdgeRef): Promise<Edge> {
        let edge: Edge = this.edges.get(ref)
        if (edge === undefined) {
            edge = await this.edgeGet(await this.rootGet(), ref)
            this.edges.set(ref, edge)
        }
        return edge
    }

    async getEdgeRange(startRef: EdgeRef, edgeCount: number): Promise<Edge[]> {
        const edges: Edge[] = await this.edgeRange(
            await this.rootGet(),
            startRef,
            edgeCount
        )
        let edgeRef = startRef
        for (let i = 0; i < edgeCount; i++) {
            this.edges.set(edgeRef, edges[i])
            edgeRef += OFFSET_INCREMENTS.EDGE_INCREMENT
        }
        return edges
    }

    async getProp(ref: PropRef): Promise<Prop> {
        let prop: Prop = this.props.get(ref)
        if (prop === undefined) {
            prop = await this.propGet(await this.rootGet(), ref)
            this.props.set(ref, prop)
        }
        return prop
    }

    async getPropRange(startRef: PropRef, propCount: number): Promise<Prop[]> {
        const props: Prop[] = await this.propRange(
            await this.rootGet(),
            startRef,
            propCount
        )
        let propRef = startRef
        for (let i = 0; i < propCount; i++) {
            this.props.set(propRef, props[i])
            propRef += OFFSET_INCREMENTS.PROP_INCREMENT
        }
        return props
    }

    async getIndex(ref: IndexRef): Promise<Index> {
        let index: Index = this.indices.get(ref)
        if (index === undefined) {
            index = await this.indexGet(await this.rootGet(), ref)
            this.indices.set(ref, index)
        }
        return index
    }

    async allVertices(): Promise<Vertex[]> {
        return await this.verticesAll(await this.rootGet())
    }

    async allEdges(): Promise<Edge[]> {
        return await this.edgesAll(await this.rootGet())
    }

    async allProps(): Promise<Prop[]> {
        return await this.propsAll(await this.rootGet())
    }

    async matchAnyEdgeProp(
        edge: Edge,
        predicate: PropPredicate
    ): Promise<boolean> {
        if (edge.nextProp !== undefined)
            return await this.matchNextProp(edge.nextProp, predicate)
        else return false
    }

    async matchAnyVertexProp(
        vertex: Vertex,
        predicate: PropPredicate
    ): Promise<boolean> {
        if (vertex.nextProp !== undefined)
            return await this.matchNextProp(vertex.nextProp, predicate)
        else return false
    }

    async matchNextProp(
        ref: PropRef,
        predicate: PropPredicate
    ): Promise<boolean> {
        let prop: Prop = await this.getProp(ref)
        if (
            predicate.keyType === prop.key &&
            predicate.operation.predicate(prop.value)
        ) {
            return true
        } else if (prop.nextProp !== undefined) {
            return await this.matchNextProp(prop.nextProp, predicate)
        }
        return false
    }

    async matchAnyIndex(vertex: Vertex, key: KeyType): Promise<Index> {
        return await this.matchNextIndex(vertex.nextIndex, key)
    }

    async matchNextIndex(
        ref: IndexRef,
        key: KeyType
    ): Promise<Index | undefined> {
        if (ref !== undefined) {
            const index: Index = await this.getIndex(ref)
            if (index.key === key) return index
            else return await this.matchNextIndex(index.nextIndex, key)
        } else return undefined
    }

    async searchIndex(
        index: Index,
        predicate: PropPredicate
    ): Promise<IndexedValue> {
        const { keyType, operation } = predicate
        const indexedValue: IndexedValue = await this.indexSearch(
            index.value,
            operation.operand
        )
        return indexedValue
    }

    async getVertexEdges(vertex: Vertex): Promise<Edge[]> {
        const edges: Edge[] = []
        if (vertex.nextEdge !== undefined) {
            await this.getNextEdges(vertex.nextEdge, edges)
        }
        return edges
    }

    async getNextEdges(ref: EdgeRef, edges: Edge[]): Promise<void> {
        const edge: Edge = await this.getEdge(ref)
        edges.push(edge)
        if (edge.nextEdge !== undefined) {
            await this.getNextEdges(edge.nextEdge, edges)
        }
    }

    async getVertexProps(vertex: Vertex): Promise<Prop[]> {
        const props: Prop[] = []
        if (vertex.nextProp !== undefined) {
            await this.getNextProps(vertex.nextProp, props)
        }
        return props
    }

    async getEdgeProps(edge: Edge): Promise<Prop[]> {
        const props: Prop[] = []
        if (edge.nextProp !== undefined) {
            await this.getNextProps(edge.nextProp, props)
        }
        return props
    }

    async getNextProps(ref: PropRef, props: Prop[]): Promise<void> {
        const prop: Prop = await this.getProp(ref)
        props.push(prop)
        if (prop.nextProp !== undefined) {
            await this.getNextProps(prop.nextProp, props)
        }
    }

    tx() {
        return new Tx(this)
    }
    /**
     * Pack a complete graph
     * @param versionRoot - the version root
     * @returns - a block containing the packed graph
     */
    async packGraphVersion(versionRoot?: Link): Promise<Block> {
        const { root } =
            versionRoot === undefined
                ? await this.rootGet()
                : { root: versionRoot }
        return await this.packGraph(root)
    }

    /**
     * Pack a graph fragment
     *
     * @param vertexOffsetStart - the offset of the first vertex
     * @param vertexCount - the number of vertices, counting from the first vertex
     * @param graphDepth - the depth of the fragment
     * @param versionRoot - the version root
     * @returns - a block containing the packed graph fragment
     */
    async packGraphFragment(
        vertexOffsetStart: number,
        vertexCount: number,
        graphDepth: number,
        versionRoot?: Link
    ): Promise<Block> {
        const { root } =
            versionRoot === undefined
                ? await this.rootGet()
                : { root: versionRoot }
        return await this.packComputed(
            root,
            vertexOffsetStart,
            vertexCount,
            graphDepth
        )
    }
}

class Tx implements ElementAccessor {
    graph: Graph

    vertices: { added: Map<number, Vertex>; updated: Map<number, Vertex> }
    edges: { added: Map<number, Edge>; updated: Map<number, Edge> }
    props: { added: Map<number, Prop>; updated: Map<number, Prop> }
    indices: { added: Map<number, Index>; updated: Map<number, Index> }

    vertexOffsetInit: number
    edgeOffsetInit: number
    propOffsetInit: number
    indexOffsetInit: number

    vertexOffset: number
    edgeOffset: number
    propOffset: number
    indexOffset: number

    constructor(graph: Graph) {
        this.graph = graph
    }

    async start(): Promise<Tx> {
        this.vertices = { added: new Map(), updated: new Map() }
        this.edges = { added: new Map(), updated: new Map() }
        this.props = { added: new Map(), updated: new Map() }
        this.indices = { added: new Map(), updated: new Map() }
        const { root, index } = await this.graph.rootGet()
        const { vertexOffset, edgeOffset, propOffset, indexOffset } =
            root !== undefined
                ? await this.graph.offsetsGet({ root, index })
                : {
                      vertexOffset: 0,
                      edgeOffset: 0,
                      propOffset: 0,
                      indexOffset: 0,
                  }
        this.vertexOffsetInit = vertexOffset
        this.edgeOffsetInit = edgeOffset
        this.propOffsetInit = propOffset
        this.indexOffsetInit = indexOffset
        this.vertexOffset = vertexOffset
        this.edgeOffset = edgeOffset
        this.propOffset = propOffset
        this.indexOffset = indexOffset
        return this
    }

    nextVertexOffset(): Offset {
        const currentOffset = this.vertexOffset
        this.vertexOffset += OFFSET_INCREMENTS.VERTEX_INCREMENT
        return currentOffset
    }

    nextEdgeOffset(): Offset {
        const currentOffset = this.edgeOffset
        this.edgeOffset += OFFSET_INCREMENTS.EDGE_INCREMENT
        return currentOffset
    }

    nextPropOffset(): Offset {
        const currentOffset = this.propOffset
        this.propOffset += OFFSET_INCREMENTS.PROP_INCREMENT
        return currentOffset
    }

    nextIndexOffset(): Offset {
        const currentOffset = this.indexOffset
        this.indexOffset += OFFSET_INCREMENTS.INDEX_INCREMENT
        return currentOffset
    }

    async getVertex(ref: VertexRef): Promise<Vertex> {
        let vertex: Vertex
        if (ref >= this.vertexOffsetInit) {
            vertex = this.vertices.added.get(ref)
            if (vertex === undefined) vertex = this.vertices.updated.get(ref)
            if (vertex === undefined)
                throw new Error(`invalid vertex ref ${ref}`)
        } else vertex = await this.graph.getVertex(ref)
        return vertex
    }

    async getEdge(ref: EdgeRef): Promise<Edge> {
        let edge: Edge
        if (ref >= this.edgeOffsetInit) {
            edge = this.edges.added.get(ref)
            if (edge === undefined) edge = this.edges.updated.get(ref)
            if (edge === undefined) throw new Error(`invalid edge ref ${ref}`)
        } else edge = await this.graph.getEdge(ref)
        return edge
    }

    async getProp(ref: PropRef): Promise<Prop> {
        let prop: Prop
        if (ref >= this.propOffsetInit) {
            prop = this.props.added.get(ref)
            if (prop === undefined) prop = this.props.updated.get(ref)
            if (prop === undefined) throw new Error(`invalid prop ref ${ref}`)
        } else prop = await this.graph.getProp(ref)
        return prop
    }

    async getIndex(ref: IndexRef): Promise<Index> {
        let index: Index
        if (ref >= this.propOffsetInit) {
            index = this.indices.added.get(ref)
            if (index === undefined) index = this.indices.updated.get(ref)
            if (index === undefined) throw new Error(`invalid index ref ${ref}`)
        } else index = await this.graph.getProp(ref)
        return index
    }

    notifyVertexUpdate(vertex: Vertex) {
        if (!this.vertices.added.has(vertex.offset)) {
            this.vertices.updated.set(vertex.offset, vertex)
        }
    }

    notifyEdgeUpdate(edge: Edge) {
        if (!this.edges.added.has(edge.offset)) {
            this.edges.updated.set(edge.offset, edge)
        }
    }

    notifyPropUpdate(prop: Prop) {
        if (!this.props.added.has(prop.offset)) {
            this.props.updated.set(prop.offset, prop)
        }
    }

    notifyIndexUpdate(index: Index) {
        if (!this.indices.added.has(index.offset)) {
            this.indices.updated.set(index.offset, index)
        }
    }

    addVertex(type?: VertexType): Vertex {
        const offset = this.nextVertexOffset()
        const vertex: Vertex = { status: Status.CREATED, offset }
        if (type !== undefined) vertex.type = type
        this.vertices.added.set(offset, vertex)
        return vertex
    }

    async addEdge(
        source: Vertex,
        target: Vertex,
        type?: EdgeType
    ): Promise<Edge> {
        const offset = this.nextEdgeOffset()
        const edge: Edge = {
            status: Status.CREATED,
            offset,
            source: source.offset,
            target: target.offset,
        }
        if (type !== undefined) edge.type = type
        this.edges.added.set(edge.offset, edge)
        if (source.nextEdge !== undefined) {
            const nextEdge: Edge = await this.getEdge(source.nextEdge)
            await this.appendEdge(nextEdge, edge)
        } else {
            source.nextEdge = offset
            this.notifyVertexUpdate(source)
        }
        return edge
    }

    async linkVertexEdge(source: Vertex, edgeRef: EdgeRef): Promise<void> {
        const edge: Edge = await this.getEdge(edgeRef)
        if (source.nextEdge !== undefined) {
            const nextEdge: Edge = await this.getEdge(source.nextEdge)
            await this.appendEdge(nextEdge, edge)
        } else {
            source.nextEdge = edgeRef
            this.notifyVertexUpdate(source)
        }
    }

    async addVertexProp(
        vertex: Vertex,
        key: KeyType,
        value: PropValue,
        type?: PropType
    ): Promise<Prop> {
        const offset = this.nextPropOffset()
        const prop: Prop = { status: Status.CREATED, offset, key, value }
        if (type !== undefined) prop.type = type
        this.props.added.set(prop.offset, prop)
        if (vertex.nextProp !== undefined) {
            const nextProp: Prop = await this.getProp(vertex.nextProp)
            await this.appendProp(nextProp, prop)
        } else {
            vertex.nextProp = offset
            this.notifyVertexUpdate(vertex)
        }
        return prop
    }

    async linkVertexProp(source: Vertex, propRef: PropRef): Promise<void> {
        const prop: Prop = await this.getProp(propRef)
        if (source.nextProp !== undefined) {
            const nextProp: Prop = await this.getProp(source.nextProp)
            await this.appendProp(nextProp, prop)
        } else {
            source.nextProp = propRef
            this.notifyVertexUpdate(source)
        }
    }

    async addVertexIndex(
        vertex: Vertex,
        key: KeyType,
        value: Link,
        type?: IndexType
    ): Promise<Index> {
        const offset = this.nextIndexOffset()
        const index: Index = { status: Status.CREATED, offset, key, value }
        if (type !== undefined) index.type = type
        this.indices.added.set(index.offset, index)
        if (vertex.nextIndex !== undefined)
            await this.appendVertexIndex(vertex.nextIndex, index)
        else {
            vertex.nextIndex = offset
            this.notifyVertexUpdate(vertex)
        }
        return index
    }

    async addEdgeProp(
        edge: Edge,
        key: KeyType,
        value: PropValue,
        type?: PropType
    ): Promise<Prop> {
        const offset = this.nextPropOffset()
        const prop: Prop = { status: Status.CREATED, offset, key, value }
        if (type !== undefined) prop.type = type
        this.props.added.set(prop.offset, prop)
        if (edge.nextProp !== undefined) {
            const nextProp: Prop = await this.getProp(edge.nextProp)
            await this.appendProp(nextProp, prop)
        } else {
            edge.nextProp = offset
            this.notifyEdgeUpdate(edge)
        }
        return prop
    }

    async linkEdgeProp(edge: Edge, propRef: PropRef): Promise<void> {
        const prop: Prop = await this.getProp(propRef)
        if (edge.nextProp !== undefined) {
            const nextProp: Prop = await this.getProp(edge.nextProp)
            await this.appendProp(nextProp, prop)
        } else {
            edge.nextProp = propRef
            this.notifyEdgeUpdate(edge)
        }
    }

    async linkEdge(currentEdge: Edge, edgeRef: EdgeRef): Promise<void> {
        const edge: Edge = await this.getEdge(edgeRef)
        await this.appendEdge(currentEdge, edge)
    }

    async appendEdge(currentEdge: Edge, newEdge: Edge): Promise<void> {
        if (currentEdge.offset === newEdge.offset)
            throw new Error(`Invalid edge append. Cannot append to itself`)
        if (currentEdge.sourceNext !== undefined) {
            const sourceNext: Edge = await this.getEdge(currentEdge.sourceNext)
            await this.appendEdge(sourceNext, newEdge)
        } else {
            currentEdge.sourceNext = newEdge.offset
            newEdge.sourcePrev = currentEdge.offset
            this.notifyEdgeUpdate(currentEdge)
        }
    }

    async linkProp(currentProp: Prop, propRef: PropRef): Promise<void> {
        const prop: Prop = await this.getProp(propRef)
        await this.appendProp(currentProp, prop)
    }

    async appendProp(currentProp: Prop, newProp: Prop): Promise<void> {
        if (currentProp.offset === newProp.offset)
            throw new Error(`Invalid prop append. Cannot append to itself`)
        if (currentProp.nextProp !== undefined) {
            const nextProp: Prop = await this.getProp(currentProp.nextProp)
            await this.appendProp(nextProp, newProp)
        } else {
            currentProp.nextProp = newProp.offset
            this.notifyPropUpdate(currentProp)
        }
    }

    async appendVertexIndex(
        currentRef: IndexRef,
        newIndex: Index
    ): Promise<void> {
        const currentIndex: Index = await this.getIndex(currentRef)
        if (currentIndex.offset === newIndex.offset)
            throw new Error(`Invalid index append. Cannot append to itself`)
        if (currentIndex.nextIndex !== undefined) {
            await this.appendVertexIndex(currentIndex.nextIndex, newIndex)
        } else {
            currentIndex.nextIndex = newIndex.offset
            this.notifyIndexUpdate(currentIndex)
        }
    }

    async uniqueIndex(
        outgoingFrom: Vertex,
        key: KeyType,
        type?: IndexType
    ): Promise<Index | undefined> {
        if (this.graph.indexCreate === undefined)
            throw new Error('Please provide an index store to the graph')
        const edgesOut = edgesOutgoing(outgoingFrom, this)
        const values: IndexedValue[] = []
        for await (const edge of edgesOut) {
            const targetVertex: Vertex = await this.getVertex(edge.target)
            const targetVertexProps = propsNext(targetVertex.nextProp, this)
            for await (const prop of targetVertexProps) {
                const propKey: KeyType = prop.key
                if (propKey === key) {
                    const value = prop.value
                    values.push({ value, ref: edge.offset })
                    break
                }
            }
        }
        if (values.length === 0) return undefined
        else {
            const link: Link = await this.graph.indexCreate(values)
            const index: Index = await this.addVertexIndex(
                outgoingFrom,
                key,
                link,
                type
            )
            return index
        }
    }

    async commit({
        comment,
        tags,
        signer,
    }: {
        comment?: Comment
        tags?: Tag[]
        signer?: Signer
    }): Promise<{ root: Link; index: RootIndex; blocks: Block[] }> {
        const verticesNew = new Map([
            ...this.graph.vertices,
            ...this.vertices.updated,
            ...this.vertices.added,
        ])
        const edgesNew = new Map([
            ...this.graph.edges,
            ...this.edges.updated,
            ...this.edges.added,
        ])
        const propsNew = new Map([
            ...this.graph.props,
            ...this.props.updated,
            ...this.props.added,
        ])
        const indicesNew = new Map([
            ...this.graph.indices,
            ...this.indices.updated,
            ...this.indices.added,
        ])
        const sourceVertexRefs: Set<VertexRef> = new Set(
            Array.from(edgesNew.values()).map((edge) => edge.source)
        )
        const targetVertexRefs: Set<VertexRef> = new Set(
            Array.from(edgesNew.values()).map((edge) => edge.target)
        )
        const editedVertexRefs: Set<VertexRef> = new Set(
            Array.from(verticesNew.keys())
        )
        const impactedVertices = new Set([
            ...sourceVertexRefs,
            ...targetVertexRefs,
            ...editedVertexRefs,
        ])

        await this.fillTargetLinks(impactedVertices)

        const rootBefore = await this.graph.rootGet()

        const { root, index, blocks } = await this.graph.commit(rootBefore, {
            vertices: this.vertices,
            edges: this.edges,
            props: this.props,
            indices: this.indices,
        })

        const versionDetails: VersionDetails = {
            comment,
            tags,
            timestamp: Date.now(),
        }

        if (signer !== undefined) {
            const signature = await signer.sign(root)
            versionDetails.signature = base64.fromByteArray(signature)
            const publicKey = await signer.exportPublicKey()
            versionDetails.publicKey = publicKey
            if (signer.name !== undefined) {
                versionDetails.author = signer.name
            }
            if (signer.email !== undefined) {
                versionDetails.email = signer.email
            }
        }

        const version: Version = {
            root,
            parent: rootBefore.root,
            details: versionDetails,
        }

        await this.graph.versionSet({ version, index })

        this.graph.vertices = verticesNew
        this.graph.edges = edgesNew
        this.graph.props = propsNew
        this.graph.indices = indicesNew

        return { root, index, blocks }
    }

    async fillTargetLinks(refs: Iterable<VertexRef>): Promise<void> {
        const incomingEdges = new Map<VertexRef, Set<EdgeRef>>()
        await traverseVertices(this, refs, incomingEdgesVisitor(incomingEdges))
        for (const [vertexRef, incomingRefs] of incomingEdges.entries()) {
            const vertex: Vertex = await this.getVertex(vertexRef)
            if (incomingRefs !== undefined) {
                const outgoingEdges = edgesOutgoing(vertex, this)
                // FIXME Poor alg assumes single incoming edge (ie tree struct), elaboration needed ...
                // Should we to follow Neo4j's RelationshipRecord structure to store
                // a bool when edge is first in chain (for both - src and target chains) ?
                for await (const outgoingEdge of outgoingEdges) {
                    for (const incomingEdgeRef of incomingRefs.values()) {
                        const incomingEdge: Edge = await this.getEdge(
                            incomingEdgeRef
                        )
                        outgoingEdge.targetPrev = incomingEdge.offset
                        if (outgoingEdge.offset === vertex.nextEdge) {
                            incomingEdge.targetNext = outgoingEdge.offset
                        }
                    }
                }
            }
        }
    }
}

export { Graph, Tx, ElementAccessor }
