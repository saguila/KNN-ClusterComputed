def mean(data,weight):
 solution = {
	"Proximity":prox_weight(data),
	"Same":same_weight(data),
	"Linear":linear_weight(data)
		 }
 return solution.get(weight,"Wrong weight")



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
 


def same_weight(data):
 sum=0
 div=0
 for i in range (0,len(data)):
  sum+=data[i][2]
  div+=1
 return sum/div 



 
def linear_weight(data):
 sum=0
 div=0
 k=len(data)
 for i in range (0,len(data)):
  sum+=data[i][2]*(k-i)
  div+=(k-i)
 return sum/div
 
