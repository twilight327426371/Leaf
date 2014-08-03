
from leaf.model import Entry, Leaderboard
import unittest

class EntryTest(unittest.TestCase):

    def test_entry_new(self):
        e = Entry(2, 1, 12)
        self.assertEqual((e.leaderboard_id, e.entry_id, e.score, e.rank), (1, 2, 12, None))
        e = Entry(2, 1, 12, 2)
        self.assertEqual((e.leaderboard_id, e.entry_id, e.score, e.rank), (1, 2, 12, 2))

    def test_as_json(self):
        e = Entry(2, 1, 12, 2).as_json()
        self.assertEqual((e['leaderboard_id'], e['entry_id'], e['score'], e['rank']), (1, 2, 12, 2))


class LeaderboardTest(unittest.TestCase):

    def test_entry_new(self):
        lb = Leaderboard('test', 1)
        self.assertEqual((lb.name, lb.leaderboard_id), ('test', 1))
        lb = Leaderboard('test')
        self.assertEqual((lb.name, lb.leaderboard_id), ('test', None))

    def test_as_json(self):
        lb = Leaderboard('test', 1).as_json()
        self.assertEqual((lb['name'], lb['leaderboard_id']), ('test', 1))

if __name__ == '__main__':
    unittest.main()