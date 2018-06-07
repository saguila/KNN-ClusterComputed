
import math
import numpy as np
import rlcompleter, readline
readline.parse_and_bind('tab:complete')

 
def knn_elements(x,n,d):
 rdd=[]
 long=n*d
 for i in range(d):
  rdd.append(x.map(lambda (k,l) : (k+i, l)).cache())
 k=sc.union(rdd)
 for i in range(d):
  rdd[i].unpersist()
 return k.reduceByKey(lambda x,y:(x+" "+y)).filter(lambda x:len(x[1])==long).sortByKey(False,1).cache()
 



def deleteHeader(idx, iter):
 output=[]
 for sublist in iter:
  output.append(sublist)
 if idx>0:
  return(output)
 else:
  return(output[1:])



 
#3.161.058 filas (Incluyendo el header)
bitcoin = spark.sparkContext.textFile('hdfs:///loudacre/kb/bigBT.csv')
#100 filas (Incluyendo el header)
#bitcoin = spark.sparkContext.textFile('hdfs:///loudacre/kb/smallBt.csv')
bitcoinWithoutHeader = bitcoin.mapPartitionsWithIndex(deleteHeader)
#bitcoinWithoutHeader = bitcoinWithoutHeader.map(lambda line :np.array(line.split(',')).astype(np.float))
#x=bitcoinWithoutHeader.map(lambda line:(idGetter(line),dataConvert(line)))
x=bitcoinWithoutHeader.zipWithIndex().map(lambda (i,u):(u,i)).cache()
n=len(x.first()[1])
d=3
knn_elements(x,n,d).first()	

#k.saveAsTextFile('hdfs:///loudacre/kb/ff')	
