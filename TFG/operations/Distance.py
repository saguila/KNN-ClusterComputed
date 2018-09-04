import math
 
'''
Calculate's the euclidean distance of two vectors
@Params two vector (x,y)
@Return the euclidean distance value
'''
def euclidean_dist(x,y):
    dist = 0
    for i in range(0, len(x) if len(x) > len(y) else len(y)):
		for j in range(0,len(x[i][1])):
			dist += (x[i][1][j] - y[i][1][j])**2
    return math.sqrt(dist)

'''
Calculate's the manhattan distance of two vectors
@Params two vector (x,y)
@Return the manhattan distance value
'''
def manhattan_dist(x,y):
	dist = 0
	for i in range(0, len(x) if len(x) > len(y) else len(y)):
		for j in range(0,len(x[i][1])):
			dist += abs(x[i][1][j] - y[i][1][j])
	return dist


'''
Calculate's the canberra distance of two vectors
@Params two vector (x,y)
@Return the canberra distance value
'''
def canberra_dist(x,y):
	dist = 0
	try:
		for i in range(0, len(x) if len(x) > len(y) else len(y)):
			for j in range(0,len(x[i][1])):
				if(x[i][1][j]!=0 and y[i][1][j]!=0):
					dist += (abs(x[i][1][j] - y[i][1][j]))/(abs(x[i][1][j])+abs(y[i][1][j]))
		return dist
	except ZeroDivisionError:
		print("error canberra")


'''
Generates a distributed distance matrix from Brodcast values and time serie in a RDD using a type of distance
@Params Iterator of the time serie (it), broadcast values(knn_train),type of distance(distance), number of neighbours (k)
@Return a distributed distance matrix
'''
def Mdistance(it,knn_train,distance,k):
	data1=[]
	data=[]
	cont=0
	for i in range(0,len(knn_train.value)):
		data1.append(((knn_train.value[i][0],knn_train.value[i][1][1][1][0]),[]))
	for fila in it:
		++cont
		for i in range(0,len(knn_train.value)):
			if(knn_train.value[i][0]>fila[0]):
				dist = {
				"Euclidean":euclidean_dist(fila[1][0],knn_train.value[i][1][0]),
				"Manhattan":manhattan_dist(fila[1][0],knn_train.value[i][1][0]),
				"Canberra":canberra_dist(fila[1][0],knn_train.value[i][1][0])
				}
				actualData=[dist.get(distance,"Wrong distance"),-fila[0],fila[1][1][1][0]]
				if k>=cont:
					data1[i][1].append(actualData)
					data1[i]=(data1[i][0],sorted(data1[i][1])[:k])
				else:
					if actualdata[0]< data[i][1][k-1][0] or (actualdata[0]==data[i][1][k-1][0] and actualData[1] < data[i][1][k-1][1]):
						data1[i][1].append(actualData)
						data[i]=(data[i][0],sorted(data1[i][1])[:k])
	for i in range(0,len(knn_train.value)):
		data.append((data1[i][0],data1[i][1][:k]))
	return iter(data)
