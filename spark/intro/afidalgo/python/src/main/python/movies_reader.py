from pyspark import SparkConf,SparkContext
from years_counter import YearsCounter
from word_counter import WordCounter



class Reader :

	def __init__(self):
	   conf = SparkConf().setAppName("appTest").setMaster("local[*]")
	   self._sc = SparkContext(conf=conf)

	def stop(self):
	   self._sc.stop()

	def read(self,filePlace,type):
	   movies = self._sc.textFile(filePlace)
	   counter = WordCounter()
	   if type is 'Years' :
	   	  counter = YearsCounter()
	   return counter.getMaxValues(movies)   
