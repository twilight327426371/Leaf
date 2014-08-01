
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

    def find(self, lid, eid):
        data = db.query_one('SELECT eid, lid, score FROM entries WHERE lid=%s AND eid=%s', (lid, eid))
        if data:
            return self._load(data)

    def rank_for_users(self, leaderboard_id, entry_ids, dense):
        """Get the rank for by users"""
        ranks = []
        sql = self._build_rank_sql(dense)
        for entry_id in entry_ids:
            data = db.query_one(sql, leaderboard_id, entry_id)
            if data:
                ranks.append(self._load(data))
        return ranks

    def rank_for_user(self, lid, eid, dense=False):
        sql = self._build_rank_sql(dense)
        data = db.query_one(sql, (lid, eid))
        if data:
            return self._load(data)

    def _build_rank_sql(self, dense=False):
        sql = self.RANK_SQL % (('', '(ei.score, ei.eid) >= (eo.score, eo.eid)') if dense else (' + 1', 'ei.score > eo.score'))
        sql += '\nWHERE  lid=%s AND eid =%s'
        return sql

    def _rank_entries(self, entries, dense=False, offset=0):
        rank = offset + 1
        prev_entry = entries[0]
        prev_entry.rank = rank
        for e in entries[1:]:
            if dense:
                rank += 1
            elif e.score != prev_entry.score:
                rank += 1
            e.rank = rank
            prev_entry = e

    def around_me(self, leaderboard_id, entry_id, bound=5):
        pass

    def total(self, leaderboard_id):
        data = db.query_one('SELECT COUNT(1) FROM entries WHERE lid=%s', (leaderboard_id,))
        return data[0]

    def _load(self, data):
        return Entry(*data)

    def save(self, entry):
        return db.execute('INSERT INTO entries (eid, lid, score) VALUES (%s, %s, %s) \
			ON DUPLICATE KEY UPDATE score=VALUES(score)',
                          (entry.leaderboard_id, entry.entry_id, entry.score))


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
                              (leaderboard.leaderboard_id, leaderboard.name,))
