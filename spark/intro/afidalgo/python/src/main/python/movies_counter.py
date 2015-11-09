import re

class Counters:


	def year_with_more_movies(self,rdd):
		 rdd =  rdd.map(lambda line :  (re.search('[(][0-9][0-9][0-9][0-9][)]',line).group(),1))
		 rdd = rdd.reduceByKey(lambda a, b: a+b)
		 return rdd.takeOrdered(1, key=lambda x: -x[1])[0][0]