
from leaf.model import Entry, Leaderboard
from leaf import db


class EntryThing(object):

    RANK_SQL = """SELECT  eo.*,
        (
        SELECT  COUNT(%sei.score) %s
        FROM    entries ei
        WHERE  eo.lid=ei.lid AND %s
        ) AS rank
FROM   entries eo"""

    def __init__(self):
        pass

    def find(self, lid, eid):
        data = db.query_one('SELECT eid, lid, score FROM entries WHERE lid=%s AND eid=%s', (lid, eid))
        if data:
            return self._load(data)

    def find_by_score(self, leaderboard_id, score):
        results = db.query('SELECT eid, lid, score FROM entries WHERE lid=%s AND score=%s', (leaderboard_id, score))
        if results:
            return [self._load(data) for data in results]

    def rank_for_users(self, leaderboard_id, entry_ids, dense=False):
        """Get the rank for by users"""
        ranks = []
        sql = self._build_rank_sql(dense)
        for entry_id in entry_ids:
            data = db.query_one(sql, (leaderboard_id, entry_id))
            if data:
                ranks.append(self._load(data))
        return ranks

    def rank_for_user(self, lid, eid, dense=False):
        sql = self._build_rank_sql(dense)
        data = db.query_one(sql, (lid, eid))
        if data:
            return self._load(data)

    def _build_rank_sql(self, dense=False):
        sql = self.RANK_SQL % (('', '', '(ei.score, ei.eid) >= (eo.score, eo.eid)') if dense else ('DISTINCT ', ' + 1', 'ei.score > eo.score'))
        sql += '\nWHERE lid=%s AND eid=%s'
        return sql

    def rank_at(self, leaderboard_id, rank, dense=False):
        res = self.rank(leaderboard_id, 1, rank -1 , dense)
        if res and not dense:
            res = res.pop()
            entries = self.find_by_score(leaderboard_id,res.score)
            for entry in entries:
                entry.rank = res.rank
            return entries
        return res

    def rank(self, leaderboard_id, limit=1000, offset=0, dense=False):
        sql = 'SELECT * FROM entries WHERE lid=%s '
        if dense:
            sql += 'ORDER BY score DESC, eid DESC'
        else:
            sql += 'GROUP BY score, eid ORDER BY score DESC'

        sql += ' LIMIT %s OFFSET %s'
        res = db.query(sql, (leaderboard_id, limit, offset))
        res = [self._load(data) for data in res]
        if res:
            if not dense:
                entry = self.rank_for_user(leaderboard_id, res[0].entry_id, dense)
                offset = entry.rank
            else:
                offset += 1
            self._rank_entries(res, dense, offset)
        return res

    def _rank_entries(self, entries, dense=False, rank=0):
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

    def delete(self, leaderboard_id, entry_id):
        return db.execute('DELETE FROM entries WHERE lid=%s AND eid=%s', (leaderboard_id, entry_id))


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

    def delete(self, leaderboard):
        db.execute('DELETE FROM entries WHERE lid=%s', (leaderboard.leaderboard_id,))
        db.execute('DELETE FROM leaderboards WHERE lid=%s', (leaderboard.leaderboard_id,))
