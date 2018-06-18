
#Transforma a array de floats cada linea del RDD.

def transformFloat(k,d):
 l=[]
 for i in range(0,d):
  p=np.array(k[i].split(",")).astype(np.float)
  for j in range(0,len(p)):
   l.append(p[j])
 return l

#Genera una distancia euclidea entre puntos (Por algun motivo de cambio de variable no funciona bien). 

def euclidea_dist(x,y):
 dist = 0
 for i in range(0,len(y)):
  val=(x[i]-y[i])
  print(val)
  dist += val**2
  print(dist)
 return math.sqrt(dist)
 
#Devuelve un rdd con las distancia euclidea para el siguente punto.

def distanceMatrix(x,k):
  p=x.count()
  RDD=x.map(lambda (x,y):(x,euclidea_dist(y,k)))
  return RDD
 