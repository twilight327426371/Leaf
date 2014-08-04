
from leaf.model import Entry, Leaderboard
from leaf import db
from collections import namedtuple
import time
import logging
from leaf.thing.base import EntryThingTrait

LOGGER = logging.getLogger(__name__)


class TreeBucketEntryThing(EntryThingTrait):

    def sort(self, leaderboard_id):
        print 'xxx'
        start_time = time.time()

        res = db.query_one('SELECT max(score) as max_score, min(score) as min_score \
            FROM entries WHERE lid=%s', (leaderboard_id,))
        if not res:
            LOGGER.info('Possibly not found Leaderboard:%d', leaderboard_id)
            return
        self.clear_buckets(leaderboard_id)
        max_score, min_score = 1024 , 0
        self._sort_slice(leaderboard_id, min_score, max_score)
        LOGGER.info('Sorted Leaderboard:%s takes %f (secs)', leaderboard_id, time.time() - start_time)
        
    def _sort_slice(self, leaderboard_id, from_score, to_score, level=0):
        grap = to_score -  from_score
        if grap >1:
            print 'form_score , to_score : %d - %d' %(from_score , to_score)
            buckets = self._get_buckets(leaderboard_id, from_score, to_score, level)
            self.save_buckets(buckets)
            self._sort_slice(leaderboard_id, from_score, from_score + grap/2, -(abs(level) + 1))
            self._sort_slice(leaderboard_id, from_score + grap/2, to_score, abs(level)+1)

    def _get_buckets(self, leaderboard_id, from_score, to_score, level):
        res = db.query('SELECT COUNT(score) size FROM entries WHERE lid=%s AND %s<=score AND score<%s',
            (leaderboard_id, from_score, to_score))
        buckets = []
        for data in res:
            buckets.append(TreeBucket(data[0], leaderboard_id, from_score, to_score, level))
        return buckets

    def clear_buckets_by_score_range(self, leaderboard_id, form_score, to_score):
        return db.execute('DELETE FROM tree_buckets WHERE lid=%s AND %s<=score AND score<%s', (leaderboard_id, from_score, to_score))

    def clear_buckets(self, leaderboard_id):
        return db.execute('DELETE FROM tree_buckets WHERE lid=%s', (leaderboard_id,))

    def save_buckets(self, buckets):
        if not buckets:
            return

        sql = 'INSERT INTO tree_buckets(size, lid, from_score, to_score, level) VALUES '
        rows = []
        for bucket in buckets:
            rows.append('(%d, %d, %d, %d, %d)' % (bucket.size,
                bucket.lid, bucket.from_score, bucket.to_score, bucket.level))
        db.execute(sql + ','.join(rows))


#'from_rank', 'to_rank', 'dense'
TreeBucket = namedtuple('TreeBucket', ['size', 'lid', 'from_score', 'to_score', 'level'])

if __name__ == '__main__':
    from leaf import log
    #log.setdebug(True)
    db.setup('localhost', 'test', 'test', 'leaf', pool_opt={'minconn': 3, 'maxconn': 10})
    tb = TreeBucketEntryThing()

    tb.sort(1)
    print 'ow'
