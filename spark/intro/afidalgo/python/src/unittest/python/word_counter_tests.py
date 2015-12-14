from pyspark import SparkConf,SparkContext
from word_counter import WordCounter
import unittest

class TestWordCounter(unittest.TestCase):



	def setUp(self):
	   conf = SparkConf().setAppName("appTest").setMaster("local[*]")
	   self.sc = SparkContext(conf=conf)
	   self.counter = WordCounter() 

	def tearDown(self):
	   self.sc.stop()

	def test_when_exist_one_movie_and_counter(self):
	   movieList = ["1993::Toy Story Toy (1995)::Animation|Children's|Comedy",
	                "1993::ToyA StoryA ToyA (1995)::Animation|Children's|Comedy"]	 
	   result = (('ToyA', ['::ToyA StoryA ToyA (1995)::']),
	             ('Toy', ['::Toy Story Toy (1995)::']))                
	   movies = self.sc.parallelize(movieList)
 	   self.assertEqual(self.counter.getMaxValues(movies),result)   


 	def test_when_exist_one_movie_and_counter_moreMovies(self):
	   movieList = ["1993::Toy Story Toy (1995)::Animation|Children's|Comedy",
	                "1993::ToyA StoryB ToyA (1995)::Animation|Children's|Comedy",
	                "1993::ToyA StoryA ToyA (1995)::Animation|Children's|Comedy"]	 
	   result = (('ToyA', ['::ToyA StoryB ToyA (1995)::','::ToyA StoryA ToyA (1995)::']))                
	   movies = self.sc.parallelize(movieList)
 	   self.assertEqual(self.counter.getMaxValues(movies),result)   