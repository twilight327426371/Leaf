from leaf import db
from random import randint
from leaf import log
from leaf.thing.base import EntryThing


import unittest


log.setdebug(True)
db.setup('localhost', 'test', 'test', 'leaf', pool_opt={'minconn':3, 'maxconn':10})

class LeaderboardTest(unittest.TestCase):

	def setUp(self):
		self.e = EntryThing()

	def test_rank_by_eid(self):
		self.assertTrue(self.e.get_rank_by_eid(1, 12))
		self.assertEqual(self.e.get_rank_by_eid(2,12), None)
		self.assertEqual(self.e.get_rank_by_eid(2,1000000), None)



if __name__ == '__main__':
	unittest.main()