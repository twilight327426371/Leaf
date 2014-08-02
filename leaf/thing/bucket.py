
from leaf.model import Entry, Leaderboard
from leaf import db
from collections import namedtuple
import time
import logging

LOGGER = logging.getLogger(__name__)


CHUNK_BLOCK = 100


class BucketEntryThing(object):

    def sort(self, leaderboard_id, chunk_block=CHUNK_BLOCK):
        start_time = time.time()
        res = db.query_one('SELECT max(score) as max_score, min(score) as min_score \
            FROM entries WHERE lid=%s', (leaderboard_id,))
        max_score, min_score = res
        self.clear_buckets(leaderboard_id)
        from_score = max_score
        while (from_score - chunk_block) >= min_score:
            buckets = self._get_buckets(leaderboard_id, from_score - chunk_block, from_score)
            self.save_buckets(buckets)
            from_score -= chunk_block
        LOGGER.info('Sorted Leaderboard:%s takes %d (secs)', leaderboard_id, time.time() - start_time)

    def _get_buckets(self, leaderboard_id, from_score, to_score):
        res = db.query('SELECT score, count(score) as size FROM entries \
           WHERE lid=%s AND %s < score AND score <= %s GROUP BY score', (leaderboard_id, from_score, to_score))
        buckets = []
        for data in res:
            buckets.append(ScoreBucket(data[0], data[1], leaderboard_id))
        return buckets

    def clear_buckets_by_score_range(self, leaderboard_id, form_score, to_score):
        return db.execute('DELETE FROM score_buckets WHERE lid=%s AND %s < score AND score <= %s', (leaderboard_id, from_score, to_score))

    def clear_buckets(self, leaderboard_id):
        return db.execute('DELETE FROM score_buckets WHERE lid=%s', (leaderboard_id,))

    def save_buckets(self, buckets):
        if not buckets:
            return

        sql = 'INSERT INTO score_buckets(score, size, lid) VALUES '
        rows = []
        for bucket in buckets:
            rows.append('(%d, %d, %d)' % (bucket.score, bucket.size, bucket.lid))
        db.execute(sql + ','.join(rows))


#'from_rank', 'to_rank', 'dense'
ScoreBucket = namedtuple('ScoreBucket', ['score', 'size', 'lid'])
