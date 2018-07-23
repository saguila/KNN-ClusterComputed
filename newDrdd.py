
def deleteHeader(idx, iter):
    output=[]
    for sublist in iter:
        output.append(sublist)
    if idx>0:
        return(output)
    else:
        return(output[1:])

def dRdd(rdd,d,n):
	return rdd.zipWithIndex().map(lambda row: dRddMapTransformFunction(row,d,n)).flatMap(lambda x : x).groupByKey().mapValues(lambda x : dRddConsolidateIterator(x,d))

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

def dRddConsolidateIterator(it,d):
	l = list(it)
	if len(l) == d:
		return (l,None)
	else:
		value = l.pop()
		return (l,value)

## Ejemplo de prueba:
Xpredict = spark.sparkContext.textFile("hdfs:///loudacre/kb/Weather.csv").mapPartitionsWithIndex(deleteHeader).map(lambda x : float(x.split(',')[1])).cache()
d = 2
n = Xpredict.count()
out = dRdd(Xpredict,d,n)