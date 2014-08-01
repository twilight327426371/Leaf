class Leaderboard(object):

	def __init__(self, leaderboard):
		self.leaderboard = leaderboard

	@property
	def leaderboard_id(self):
		return self.leaderboard.leaderboard_id

    def rank_for_users(self, entry_ids, dense=False):
        return self.leaderboard.entrything.rank_for_user(self.leaderboard_id, entry_ids, dense)

	def rank_for_user(self, entry_id, dense=False):
		return self.leaderboard.entrything.rank_for_user(self.leaderboard_id, entry_id, dense)

    def rank_at(self, rank, dense=False):
        return self.leaderboard.rank_at(self.leaderboard_id, rank, dense)

    def rank(self, limit=100, offset=0, dense=False):
        return self.leaderboard.rank(self.leaderboard_id, limit, offset, dense)

    @property
    def total(self):
        return self.leaderboard.total(self.leaderboard_id)
