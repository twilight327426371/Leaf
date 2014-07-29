class Leaderboard(object):

	def __init__(self, leaderboard):
		self.leaderboard = leaderboard

	@property
	def leaderboard_id(self):
		return self.leaderboard.leaderboard_id

	def get_rank_by_eid(self, eid, dense=False):
		return self.leaderboard.entrything.get_rank_by_eid(self.leaderboard_id, eid , dense)