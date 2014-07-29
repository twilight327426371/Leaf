
from leaf.model import Entry
from leaf import db

class EntryThing(object):

	RANK_SQL = """
SELECT  eo.*, 
        (
        SELECT  COUNT(ei.score) + 1
        FROM    entries ei
        WHERE   %s
        ) AS rank
FROM    entries eo 
WHERE  lid=%%s eid =%%s"""

	def __init__(self):
		pass

	def find(self, lid , eid):
		data = db.query_one('SELECT eid, lid, score FROM entries WHERE lid=%s AND eid=%s', (lid, eid))
		if data:
			return self._load(data)

	def get_rank_by_eid(self, lid, eid, dense=False):
		sql = self.RANK_SQL %('(ei.score, ei.eid) >= (eo.score, eo.eid)' if dense else 'ei.score > eo.score')
		data = db.query_one(sql, (lid, eid))
		if data:
			return self._load(data)

	def _load(self, data):
		return Entry(*data)

	def save(self, entry):
		return db.execute('INSERT INTO entries (eid, lid, score) VALUES (%s, %s, %s) \
			ON DUPLICATE KEY UPDATE score=VALUES(score)', (entriy.leaderboard_id, entriy.entriy_id, entriy.score))

