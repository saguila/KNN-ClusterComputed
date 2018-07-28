#Define error  Pred-> Predicted value,Value-> Real value ,error->Type of error

def error(pred,value,err):
 solution = {
	"MAE":mean_absolute_error(pred,value),
	"ME":mean_error(pred,value),
	"RMSE":root_mean_squared_error(pred,value),
	"MAPE":mean_absolute_percentage_error(pred,value),
	"MPE":mean_percentage_error(pred,value)
		 }
 return solution.get(err,"Wrong Error")

def mean_error(pred,value):
 return (value-pred)

def mean_absolute_error(pred,value):
 return abs(value-pred)

def root_mean_squared_error(pred,value):
 return (value-pred)**2

def mean_absolute_percentage_error(pred,value):
	try:
		if(value!=0):
			return abs((value-pred)/value)
		else: 
			return 0
	except ZeroDivisionError:
		print("error MAPE")

def mean_percentage_error(pred,value):
	try:
		if(value!=0):
			return (value-pred)/value
		else: 
			return 0
	except ZeroDivisionError:
		print("error MPE") 