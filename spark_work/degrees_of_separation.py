from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("Degrees of Seperation")
sc = SparkContext.getOrCreate(conf=conf)
sc.setLogLevel("WARN")

startID = 4256# spiderman
target = 443

hitCounter = sc.accumulator(0)

def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = [int(connection) for connection in fields[1:]]

    color = "WHITE"
    distance = 9999

    if heroID == startID:
        color = "GRAY"
        distance = 0
    return (heroID, (connections, distance, color))

def createStartingRDD():
    inputfile = sc.textFile("MarvelGraph.txt")
    return inputfile.map(convertToBFS)

iterationRDD = createStartingRDD()

def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    ret = []
    if color == "GRAY":
        for connection in connections:
            newCharacterId = connection
            newDist = distance + 1
            newColor = "GRAY"
            if target == connection:
                hitCounter.add(1)
            newEntry = (newCharacterId, ([], newDist, newColor))
            ret.append(newEntry)
        color = "BLACK"
    ret.append((characterID, (connections, distance, color)))
    return ret

def bfsReduce(hero1, hero2):
    connections1 = hero1[0]
    connections2 = hero2[0]
    distance1 = int(hero1[1])
    distance2 = int(hero2[1])
    color1 = hero1[2]
    color2 = hero2[2]

    distance = 9999
    color = color1
    connections = []

    if len(connections1) > 0:
        connections.extend(connections1)
    if len(connections2) > 0:
        connections.extend(connections2)
    
    if distance1 < distance:
        distance = distance1
    if distance2 < distance:
        distance = distance2

    if color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK'):
        color = color2
    if color1 == 'GRAY' and color2 == 'BLACK':
        color = color2
    if color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK'):
        color = color1
    if color2 == 'GRAY' and color1 == 'BLACK':
        color = color1
    return (connections, distance, color)

for iteration in range(0, 10):
    print("Running BFS Run # " + str(iteration + 1))

    mapped = iterationRDD.flatMap(bfsMap)
    
    mapped.count() 

    if hitCounter.value > 0:
        print("Hit the target from " + str(hitCounter.value) + " different directions")
        break

    iterationRDD = mapped.reduceByKey(bfsReduce).cache()
