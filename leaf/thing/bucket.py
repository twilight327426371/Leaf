
from leaf.model import Entry, Leaderboard
from leaf import db
from collections import namedtuple
import time
import logging
from leaf.thing.base import EntryThingTrait

LOGGER = logging.getLogger(__name__)


CHUNK_BLOCK = 100


class BucketEntryThing(EntryThingTrait):

    def rank_for_user(self, leaderboard_id, entry_id, dense=False):
        entry = self.find(leaderboard_id, entry_id)
        if entry:
            if not dense:
                data = db.query_one('SELECT dense FROM score_buckets WHERE lid=%s AND score=%s', (leaderboard_id, entry.score))
                entry.rank = data[0]
            else:
                data  = db.query_one('SELECT from_rank FROM score_buckets WHERE lid=%s AND score=%s', (leaderboard_id, entry.score))
                from_rank = data[0] 
                rank = db.query_one('SELECT  COUNT(eid) as rank FROM  entries WHERE lid=%s AND eid>%s AND score=%s', 
                    (leaderboard_id, entry_id, entry.score))[0]
                entry.rank = from_rank + rank 
        return entry


    def rank_for_users(self, leaderboard_id, entry_ids, dense=False):
        return [self.rank_for_user(leaderboard_id, entry_id, dense) for entry_id in entry_ids]


    def sort(self, leaderboard_id, chunk_block=CHUNK_BLOCK):
        start_time = time.time()
        res = db.query_one('SELECT max(score) as max_score, min(score) as min_score \
            FROM entries WHERE lid=%s', (leaderboard_id,))
        if not res:
            LOGGER.info('Possibly not found Leaderboard:%d', leaderboard_id)
            return

        max_score, min_score = res
        rank, dense = 0, 0
        self.clear_buckets(leaderboard_id)
        from_score = max_score
        while from_score >= min_score:
            buckets, rank, dense = self._get_buckets(leaderboard_id, from_score - chunk_block, from_score, rank, dense)
            self.save_buckets(buckets)
            from_score -= chunk_block
        LOGGER.info('Sorted Leaderboard:%s takes %f (secs)', leaderboard_id, time.time() - start_time)

    def _get_buckets(self, leaderboard_id, from_score, to_score, rank, dense):
        res = db.query('SELECT score, COUNT(score) size FROM entries WHERE lid=%s AND %s<score AND score<=%s GROUP BY score ORDER BY score DESC',
            (leaderboard_id, from_score, to_score))
        buckets = []
        for data in res:
            buckets.append(ScoreBucket(data[0], data[1], leaderboard_id, rank + 1, rank + data[1], dense + 1))
            rank += data[1]
            dense += 1
        return buckets, rank, dense

    def clear_buckets_by_score_range(self, leaderboard_id, form_score, to_score):
        return db.execute('DELETE FROM score_buckets WHERE lid=%s AND %s<score AND score<=%s', (leaderboard_id, from_score, to_score))

    def clear_buckets(self, leaderboard_id):
        return db.execute('DELETE FROM score_buckets WHERE lid=%s', (leaderboard_id,))

    def save_buckets(self, buckets):
        if not buckets:
            return

        sql = 'INSERT INTO score_buckets(score, size, lid, from_rank, to_rank, dense) VALUES '
        rows = []
        for bucket in buckets:
            rows.append('(%d, %d, %d, %d, %d, %d)' % (bucket.score, bucket.size,
                bucket.lid, bucket.from_rank, bucket.to_rank, bucket.dense))
        db.execute(sql + ','.join(rows))


#'from_rank', 'to_rank', 'dense'
ScoreBucket = namedtuple('ScoreBucket', ['score', 'size', 'lid', 'from_rank', 'to_rank', 'dense'])
