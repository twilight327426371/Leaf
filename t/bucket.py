from leaf import db

from leaf import log
from leaf.thing.bucket import BucketEntryThing
from leaf.model import Entry

import unittest

import data

#log.setdebug(True)
db.setup('localhost', 'test', 'test', 'leaf', pool_opt={'minconn': 3, 'maxconn': 10})

class EntryThingTraitTest(unittest.TestCase):

    def setUp(self):
        data.up(lid=2)
        self.e = BucketEntryThing()
        self.e.sort(2)

    def tearDown(self):
        data.down(lid=2)

    def test_rank_for_user(self):
        e = self.e.rank_for_user(2, 11)
        self.assertEquals((e.entry_id, e.score, e.rank), (11, 29, 5))
        e = self.e.rank_for_user(2, 13)
        self.assertEquals((e.entry_id, e.score, e.rank), (13, 29, 5))
        e = self.e.rank_for_user(2, 13, True)
        self.assertEquals((e.entry_id, e.score, e.rank), (13, 29, 11))

    def test_rank_for_users(self):
        es = self.e.rank_for_users(2, [11, 13])
        self.assertEquals((es[0].entry_id, es[0].score, es[0].rank), (11, 29, 5))
        self.assertEquals((es[1].entry_id, es[1].score, es[1].rank), (13, 29, 5))

        es = self.e.rank_for_users(2, [11, 13], True)
        self.assertEquals((es[0].entry_id, es[0].score, es[0].rank), (11, 29, 13))
        self.assertEquals((es[1].entry_id, es[1].score, es[1].rank), (13, 29, 11))

if __name__ == '__main__':
    unittest.main()