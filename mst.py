import struct
import math
import sys
import time
from mpi4py import MPI

from Classes import *

rootThread = 0


def readGraph(filename):
    with open(filename, "rb") as f:
        n = struct.unpack('i', f.read(4))[0]
        scale = int(math.log(n) / math.log(2))
        m = struct.unpack('q', f.read(8))[0]
        directed = struct.unpack('?', f.read(1))[0]
        align = struct.unpack('b', f.read(1))[0]  # 0
        rowsIndices = struct.unpack((n + 1) * 'q', f.read(8 * (n + 1)))
        endV = struct.unpack(rowsIndices[n] * 'i', f.read(rowsIndices[n] * 4))
        weights = struct.unpack(m * 'd', f.read(8 * m))

    return n, scale, m, directed, align, rowsIndices, endV, weights


def writeGraph(filename, graph):
    n = len(graph.vertices)
    m = 0
    rowsIndices = []
    endV = []
    weights = []
    for vertexFrom in graph.vertices:
        rowsIndices.append(m)
        for edge in graph.vertices[vertexFrom].edges:
            endV.append(edge.vertexTo)
            weights.append(edge.weight)
        m += len(graph.vertices[vertexFrom].edges)
    directed = False
    align = 0
    rowsIndices.append(m)
    with open(filename, "wb") as f:
        f.write(struct.pack('i', n))
        f.write(struct.pack('q', m))
        f.write(struct.pack('?', directed))
        f.write(struct.pack('b', align))
        f.write(struct.pack((n + 1) * 'q', *rowsIndices))
        f.write(struct.pack(rowsIndices[n] * 'i', *endV))
        f.write(struct.pack(m * 'd', *weights))


def writeForest(filename, trees):
    numTrees = len(trees)
    numEdges = 0
    p_edge_list = []
    edge_id = []
    for tree in trees:
        edges = tree.getEdges()
        p_edge_list.append(numEdges)
        numEdges += len(edges)
        p_edge_list.append(numEdges)
        for edge in edges:
            edge_id.append(edge.rank)
    edge_id = sorted(edge_id)
    # print(numTrees, numEdges, p_edge_list, edge_id, sep="\n")
    with open(filename, "wb") as f:
        f.write(struct.pack('i', numTrees))
        f.write(struct.pack('q', numEdges))
        f.write(struct.pack(2 * numTrees * 'q', *p_edge_list))
        f.write(struct.pack(numEdges * 'q', *edge_id))


def getGraph(filename):
    n, scale, m, directed, align, rowsIndices, endV, weights = readGraph(filename)
    vertices = {}
    for number in range(n):
        vertices[number] = Vertex(number)
    for number in range(n):
        for i in range(rowsIndices[number], rowsIndices[number + 1]):
            vertices[number].edges.append(Edge(i, number, endV[i], weights[i]))
            vertices[endV[i]].edges.append(Edge(i, endV[i], number, weights[i]))

    return Graph(vertices)


def scatterComponents(rank, np, N, comm, start, num, T):
    components = None
    if rank == rootThread:
        for number in range(1, np):
            subNum = N // np + (number < N % np)
            subStart = N // np * number + min(number, N % np)
            components = []
            for vertNum in range(subStart, subStart + subNum):
                components.append(T[vertNum])
            comm.send(components, dest=number, tag=77)
        components = []
        for vertNum in range(start, start + num):
            components.append(T[vertNum])
    else:
        components = comm.recv(components, source=rootThread, tag=77)

    return components


def syncComponents(rank, np, components, comm):
    T = []
    if rank == rootThread:
        T = components
        for number in range(1, np):
            components = None
            components = comm.recv(components, source=number, tag=77)
            for component in components:
                T.append(component)
    else:
        comm.send(components, dest=rootThread, tag=77)

    return T


def getActualComponent(curComp, components):
    if curComp.unitedWith is not None:
        for component in components:
            if component.rank == curComp.unitedWith:
                return getActualComponent(component, components)
    return curComp


def union(curComp, components):
    for component in components:
        if curComp.newMinEdge.vertexTo in component.getVerticesNumbers():
            edge = curComp.newMinEdge  # Save the edge because current component may change
            curComp = getActualComponent(curComp, components)
            component = getActualComponent(component, components)
            if curComp.rank == component.rank:  # Condition preventing loops
                return
            curComp.vertices.extend(component.vertices)
            curComp.edges.extend(component.edges)
            curComp.edges.append(edge)
            component.unitedWith = curComp.rank
            break


def getBoruvkaMST():
    start_time = time.time()
    comm = MPI.COMM_WORLD

    rank = comm.Get_rank()
    np = comm.size

    filename, outputFilename = init(sys.argv, rank)

    if np < 2:
        print('The number of MPI processes must be greater than 1')
        exit()

    T = []
    oneMoreStep = False
    if rank == rootThread:
        graph = getGraph(filename)
        # print(graph.toString(), "\n\n")
        N = len(graph.vertices)
        for number in range(N):
            T.append(Component(number, [graph.vertices[number]]))
        oneMoreStep = len(T) > 1

    oneMoreStep = comm.bcast(oneMoreStep, root=rootThread)
    step = 0
    stepLimit = 50
    while oneMoreStep and step < stepLimit:
        N = len(T)
        N = comm.bcast(N, root=rootThread)
        oneMoreStep = False
        num = N // np + (rank < N % np)  # Strong scalability is achieved due to the uniform
        start = N // np * rank + min(rank, N % np)  # distribution of components across processes
        # print(step, ")", rank, "/", np, ':', start, "-", start + num, "=", N)

        components = scatterComponents(rank, np, N, comm, start, num, T)

        for component in components:
            component.newMinEdge = Edge(-1, -1, -1, 1)
            for vertex in component.vertices:
                for edge in vertex.edges:
                    if edge.vertexTo not in component.getVerticesNumbers():
                        if edge.weight < component.newMinEdge.weight:
                            component.newMinEdge = edge

        T = syncComponents(rank, np, components, comm)

        if rank == rootThread:
            for component in T:
                if component.newMinEdge.weight != 1:
                    union(component, T)
            for i in reversed(range(len(T))):
                if T[i].unitedWith is not None:
                    del T[i]

        step += 1
        if rank == rootThread:
            for component in T:
                oneMoreStep = oneMoreStep or component.newMinEdge.weight != 1
                # print(component.toShortString())
            # print(step, ') Number of components:', len(T), oneMoreStep)
        oneMoreStep = comm.bcast(oneMoreStep, root=rootThread)
    if rank == rootThread:
        print("--- %s seconds (number of iterations: %s) ---" % (time.time() - start_time, step))
        if step == stepLimit:
            print('Iteration limit reached (' + str(step) + ')')

        trees = []
        for component in T:
            trees.append(component.convertToGraph())
        writeForest(outputFilename, trees)
        # writeGraph(outputFilename, tree)

    return


def init(argv, rank):
    if len(argv) < 2:
        if rank == rootThread:
            print("Usage:")
            print("    mpiexec -n <number_of_threads> python -m mpi4py " + argv[0] + " [options]")
            print("Options:")
            print("   -i <input_filename>")
            print("   -o <output_filename>, default value is input_filename.mst")
        exit()
    iFilename = argv[1]
    oFilename = None
    for i in range(len(argv)):
        if argv[i] == "-i":
            iFilename = argv[i + 1]
        if argv[i] == "-o":
            oFilename = argv[i + 1]

    if oFilename is None:
        oFilename = iFilename + '.mst'

    return iFilename, oFilename


getBoruvkaMST()
