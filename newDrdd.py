
def dRdd(rdd,d,n):
	return rdd.zipWithIndex().map(lambda row: dRddMapTransformFunction(row,d,n)).flatMap(lambda x : x).groupByKey().mapValues(list)


def dRddMapTransformFunction(row,d,n):
	out = []
	idx = int(row[1])
	if idx == 0:
		out.append((idx,row[0]))
	else:
		for i in range(d + 1):
			if(idx - i >= 0 and idx - i <= n - d):
				out.append((idx - i,row[0]))
	return out


## Ejemplo de prueba:
d = 2
x = spark.sparkContext.parallelize(["x0","x1","x2","x3","x4","x5","x6"])
n = x.count()
dRdd(x,d,n).sortByKey().collect()