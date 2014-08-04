
from leaf.model import Entry, Leaderboard
from leaf import db
from collections import namedtuple
import time
import logging
from leaf.thing.base import EntryThingTrait
import math

LOGGER = logging.getLogger(__name__)


class TreeBucketEntryThing(EntryThingTrait):

    def __init__(self):
        self.buckets = []

    def sort(self, leaderboard_id):
        start_time = time.time()
        res = db.query_one('SELECT max(score) as max_score, min(score) as min_score \
            FROM entries WHERE lid=%s', (leaderboard_id,))
        if not res:
            LOGGER.info('Possibly not found Leaderboard:%d', leaderboard_id)
            return
        self.clear_buckets(leaderboard_id)
        self.max_score = res[0] 
        self.min_score = res[1]
        from_score, to_score = 0, self._to_score(self.max_score)
        self._sort_slice(leaderboard_id, from_score, to_score)
        self.save_buckets()
        LOGGER.info('Sorted Leaderboard:%s takes %f (secs)', leaderboard_id, time.time() - start_time)

    def _to_score(self, max_score):
        return 2**(int(math.log(max_score, 2)) + 1)

    def _sort_slice(self, leaderboard_id, from_score, to_score, level=0):
        if from_score >= self.max_score:
            return

        grap = to_score - from_score
        self._process_buckets(leaderboard_id, from_score, to_score, level)
        if grap > 1:
            middle = (from_score + grap / 2)
            self._sort_slice(leaderboard_id, from_score, middle, -(abs(level) + 1))

            if to_score > self.max_score:
                _to_score = (middle + (to_score - middle) / 2)
                self._sort_slice(leaderboard_id, middle, _to_score, -(abs(level) + 2))
                self._sort_slice(leaderboard_id, _to_score, to_score, abs(level) + 2)
            else:
                self._sort_slice(leaderboard_id, middle, to_score, abs(level) + 1)

    def _process_buckets(self, leaderboard_id, from_score, to_score, level):
        if from_score != self.min_score and to_score > self.max_score:
            return
        to_score = min(self.max_score, to_score)
        res = db.query('SELECT COUNT(score) size FROM entries WHERE lid=%s AND %s<=score AND score<%s',
                      (leaderboard_id, from_score, to_score))
        for data in res:
            self.buckets.append(TreeBucket(data[0], leaderboard_id, from_score, to_score, level))
        if len(self.buckets) == 500:
            self.save_buckets()

    def clear_buckets_by_score_range(self, leaderboard_id, form_score, to_score):
        return db.execute('DELETE FROM tree_buckets WHERE lid=%s AND %s<=score AND score<%s', (leaderboard_id, from_score, to_score))

    def clear_buckets(self, leaderboard_id):
        return db.execute('DELETE FROM tree_buckets WHERE lid=%s', (leaderboard_id,))

    def save_buckets(self):
        if not self.buckets:
            return

        sql = 'INSERT INTO tree_buckets(size, lid, from_score, to_score, level) VALUES '
        rows = []
        for bucket in self.buckets:
            rows.append('(%d, %d, %d, %d, %d)' % (bucket.size,
                                                  bucket.lid, bucket.from_score, bucket.to_score, bucket.level))
        db.execute(sql + ','.join(rows))
        self.buckets = []


#'from_rank', 'to_rank', 'dense'
TreeBucket = namedtuple('TreeBucket', ['size', 'lid', 'from_score', 'to_score', 'level'])

if __name__ == '__main__':
    from leaf import log
    log.setdebug(False)
    db.setup('localhost', 'test', 'test', 'leaf', pool_opt={'minconn': 3, 'maxconn': 10})
    tb = TreeBucketEntryThing()
    tb.sort(1)
