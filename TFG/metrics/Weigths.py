'''
Calculate's the prediction value using a type of weighted mean.
@Param data to calculate a mean (data), the type of weight (weight)
@Return a prediction value
'''

def mean(data,weight):
 solution = {
	"Proximity":prox_weight(data),
	"Same":same_weight(data),
	"Linear":linear_weight(data)
		 }
 return solution.get(weight,"Wrong weight")

'''
Calculate's the prediction value using the proximity weighted mean.
@Param data to calculate a mean (data)
@Return a prediction value using proximity weight
'''

def prox_weight(data):
 sum=0
 div=0
 try:
  for i in range (0,len(data)):
   if(data[i][0]!=0):  
    sum+=data[i][2]*(1/data[i][0])
    div+=1/data[i][0]
  if (div!=0):
   return sum/div
  else: 
   return -1
 except ZeroDivisionError:
  print("error"+str(data))
 
 
'''
Calculate's the prediction value using the same weighted mean.
@Param data to calculate a mean (data)
@Return a prediction value using same weight
'''

def same_weight(data):
 sum=0
 div=0
 for i in range (0,len(data)):
  sum+=data[i][2]
  div+=1
 return sum/div 


'''
Calculate's the prediction value using the linear weighted mean.
@Param data to calculate a mean (data)
@Return a prediction value using linear weight
'''
 
def linear_weight(data):
 sum=0
 div=0
 k=len(data)
 for i in range (0,len(data)):
  sum+=data[i][2]*(k-i)
  div+=(k-i)
 return sum/div
 
