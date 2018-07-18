#Define error  Pred-> Predicted value,Value-> Real value ,error->Type of error

def error(pred,value,err):
 solution = {
	"MAE":mean_absolute_error(pred,value),
	"ME":mean_error(pred,value),
	"RMSE":root_mean_squared_error(pred,value),
	#"MAPE":mean_absolute_percentage_error(pred,value),
	#"MPE":mean_percentage_error(pred,value)
		 }
 return solution.get(err,"Wrong Error")

def mean_error(pred,value):
 return (pred-value)

def mean_absolute_error(pred,value):
 return abs(pred-value)

def root_mean_squared_error(pred,value):
 return (pred-value)**2

def mean_absolute_percentage_error(pred,value):
 return abs((pred-value)/pred)

def mean_percentage_error(pred,value):
 return (pred-value)/pred