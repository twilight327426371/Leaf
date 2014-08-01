
from leaf.model import Entry, Leaderboard
from leaf import db

class EntryThing(object):

	RANK_SQL = """SELECT  eo.*, 
        (
        SELECT  COUNT(ei.score) %s
        FROM    entries ei
        WHERE   %s
        ) AS rank
FROM   entries eo"""

	def __init__(self):
		pass

	def find(self, lid , eid):
		data = db.query_one('SELECT eid, lid, score FROM entries WHERE lid=%s AND eid=%s', (lid, eid))
		if data:
			return self._load(data)

	def get_rank_by_eid(self, lid, eid, dense=False):
		sql = self.RANK_SQL % (('', '(ei.score, ei.eid) >= (eo.score, eo.eid)') if dense else (' + 1', 'ei.score > eo.score'))
		sql += '\nWHERE  lid=%s AND eid =%s'
		data = db.query_one(sql, (lid, eid))
		if data:
			return self._load(data)

	def rank(self, leaderboard_id, limit=1000, offset=0, dense=False):
		sql = 'SELECT * FROM entries WHERE lid=%%s ORDER BY %s'
		if dense:
			sql = sql % ('score DESC, eid DESC',)
		else:
			sql = sql % ('score DESC',)

		sql += ' LIMIT %s OFFSET %s'
		res = db.query(sql, (leaderboard_id, limit, offset))
		res = [self._load(data) for data in res]
		if res:
			self._rank_entries(res, dense, offset)
		return res

	def _rank_entries(self, entries, dense=False, offset=0):
		rank = offset+1
		prev_entry = entries[0]
		prev_entry.rank = rank
		for e in entries[1:]:
			if dense:
					rank +=1
			elif e.score != prev_entry.score:
				rank += 1
			e.rank = rank
			prev_entry =  e

	def arround(self, leaderboard_id, entriy_id, bound=5):
		pass

	def _load(self, data):
		return Entry(*data)

	def save(self, entry):
		return db.execute('INSERT INTO entries (eid, lid, score) VALUES (%s, %s, %s) \
			ON DUPLICATE KEY UPDATE score=VALUES(score)', 
			(entriy.leaderboard_id, entriy.entriy_id, entriy.score))


class LeaderboardThing(object):

	def find(self, leaderboard_id):
		data = db.query_one('SELECT lid, name FROM leaderboards WHERE lib=%s', (leaderboard_id,))
		if data:
			return self._load(data)

	def _load(self, data):
		return Leaderboard(*data)

	def save(self, leaderboard):
		if leaderboard.leaderboard_id:
			return db.execute('INSERT INTO leaderboards (name) VALUES(%s)', leaderboard.name)
		else:
			return db.execute('INSERT INTO leaderboard (lid, name) VALUES (%s, %s) \
			ON DUPLICATE KEY UPDATE name=VALUES(name)', 
			(leaderboard.name,))
