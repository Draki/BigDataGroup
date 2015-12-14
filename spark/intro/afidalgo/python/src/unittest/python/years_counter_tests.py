from pyspark import SparkConf,SparkContext
from years_counter import YearsCounter
import unittest


class TestYearsCounter(unittest.TestCase):


	def setUp(self):
	   conf = SparkConf().setAppName("appTest").setMaster("local[*]")
	   self.sc = SparkContext(conf=conf)
	   self.counter = YearsCounter() 

	def tearDown(self):
	   self.sc.stop()

	def test_when_exist_three_movies_in_rdd(self):
		movieList = ["1993::Toy Story (1995)::Animation|Children's|Comedy",
		             "1993::Toy Story (1996)::Animation|Children's|Comedy",
		             "1993::Toy Story (1996)::Animation|Children's|Comedy"]
		movies = self.sc.parallelize(movieList)
		self.assertEqual(self.counter.getMaxValues(movies),('(1996)'))