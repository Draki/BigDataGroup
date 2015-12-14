from movies_reader import Reader
import unittest




class TestReaders(unittest.TestCase):

	def setUp(self):
	   self.reader = Reader() 

	def tearDown(self):
	   self.reader.stop()

	def test_when_read_file(self):
	   self.assertEqual(self.reader.read('/Users/alvarofidalgo/gsSpark/movies.dat','Years'),('(1996)'))
	  
