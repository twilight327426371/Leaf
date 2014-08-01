class Entry(object):

    def __init__(self, eid, lid, score, rank=None):
        self.leaderboard_id = lid
        self.entry_id = eid
        self.score = score
        self.rank = rank

    def __str__(self):
        return '<Entry leaderboard_id:%d, entry_id: %s, score: %s,  rank: %s' % (self.leaderboard_id, self.entry_id,
                self.score, self.rank)
    __repr__ = __str__

    def as_json(self):
        return self.__dict__.copy()


class Leaderboard(object):

    def __init__(self, name, leaderboard_id=None):
        self.name = name
        self.leaderboard_id = leaderboard_id
        self.entrything = None

    def as_json(self):
        return self.__dict__.copy()
