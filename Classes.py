class Graph(object):
    def __init__(self, vertices):
        self.vertices = vertices  # Dictionary

    def toString(self):
        res = 'Graph: Vertices: \n'
        vertices = []
        for i in self.vertices:
            vertices.append(self.vertices[i].toString())

        return res + ';\n'.join(vertices)

    def getEdges(self):
        edges = []
        for i in self.vertices:
            for edge in self.vertices[i].edges:
                edges.append(edge)

        return edges


class Vertex(object):
    def __init__(self, number):
        self.edges = []
        self.number = number

    def toString(self):
        res = 'Vertex ' + str(self.number) + '; Edges: \n\t'
        edges = []
        for edge in self.edges:
            edges.append(edge.toString())

        return res + ', '.join(edges)


class Edge(object):
    def __init__(self, rank, vertexFrom, vertexTo, weight):
        self.rank = rank
        self.vertexFrom = vertexFrom
        self.vertexTo = vertexTo
        self.weight = weight

    def toString(self):
        return '(' + str(self.vertexFrom) + 'x' + str(self.vertexTo) + '=' + str(self.weight) + ')'


class Component(object):
    def __init__(self, rank, vertices, edges=None):
        if edges is None:
            edges = []
        self.rank = rank
        self.vertices = vertices  # list of vertices
        self.edges = edges
        self.newMinEdge = Edge(-1, -1, -1, 1)
        self.unitedWith = None

    def getVerticesNumbers(self):
        return [v.number for v in self.vertices]

    def toString(self):
        res = 'Component: rank: ' + str(self.rank) + '; Vertices: \n'
        vertices = []
        for vertex in self.vertices:
            vertices.append(vertex.toString())
        res += ';\n'.join(vertices) + '\nEdges:\n\t'
        edges = []
        for edge in self.edges:
            edges.append(edge.toString())
        res += ', '.join(edges) + '\nnewMinEdge:\n\t'
        if self.newMinEdge is not None:
            res += self.newMinEdge.toString()

        return res + '\nUnitedWith = ' + str(self.unitedWith)

    def toShortString(self):
        return 'Component: rank: ' + str(self.rank) + '; Vertices: ' + str(len(self.vertices)) + ' Edges: ' + str(
            len(self.edges)) + ' newMinEdge: ' + str(self.newMinEdge.toString()) + '\nUnitedWith = ' + str(
            self.unitedWith)

    def convertToGraph(self):
        vertices = {}
        verticesNumbers = sorted(self.getVerticesNumbers())
        for number in verticesNumbers:
            vertices[number] = Vertex(number)
        for edge in self.edges:
            vertices[edge.vertexFrom].edges.append(edge)
        graph = Graph(vertices)

        return graph
