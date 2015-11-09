from pyspark import SparkConf,SparkContext
from movies_counter import Counters


class Reader :

	def __init__(self):
	   conf = SparkConf().setAppName("appTest").setMaster("local[*]")
	   self._sc = SparkContext(conf=conf)
	   self._counter = Counters()


	def stop(self):
	   self._sc.stop()

	def read(self,filePlace):
	   movies = self._sc.textFile(filePlace)
	   return self._counter.year_with_more_movies(movies)   
