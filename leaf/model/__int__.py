class Entry(object):

	def __init__(self, lid, eid, score, rank=None):
		self.leaderboard_id = lid
		self.entry_id = eid
		self.score = score
		self.rank = rank

	def as_json(self):
		return self.__dict__.copy()

class Leaderbaord(object):

	def __init__(self, name, leaderboard_id=None):
		self.name = name
		self.leaderboard_id = leaderboard_id

		self.entrything = None

	def as_json(self):
		return self.__dict__.copy()