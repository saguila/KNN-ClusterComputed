'''
Groups the RDD to prepare the data for the time predictions.
@Return time distance RDD.
'''
def dRdd(rdd,d,n):
    return rdd.zipWithIndex().map(lambda row: dRddMapTransformFunction(row,d,n)).flatMap(lambda x : x).groupByKey().mapValues(lambda x : dRddConsolidateIterator(x,d))

'''
Generate a map transformation for the time distance RDD.
@Return a tuple with the (timedistance ID,(timestamp and value)).
'''
def dRddMapTransformFunction(row,d,n):
    out = []
    idx = int(row[1])
    if idx == 0:
        out.append((idx,(idx,row[0])))
    else:
        for i in range(d + 1):
            if(idx - i >= 0 and idx - i <= n - d):
                out.append((idx - i,(idx,row[0])))
    return out
'''
Order and iterates the row data of the RDD using his time moument.
@Return the grouped and sorted tuples with his time prediction.
'''
def dRddConsolidateIterator(it,d):
    l = sorted(list(it))
    if len(l) == d:
        return (l,(None,[None]))
    else:
        value = l.pop()
        return (l,value)