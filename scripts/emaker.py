from leaf import db

from random import randint
from leaf import log


log.setdebug(True)
db.setup('localhost', 'test', 'test', 'leaf', pool_opt={'minconn':3, 'maxconn':10})



def create_lb(name='test'):
	r = db.query_one('SELECT lid from leaderboards WHERE name=%s', (name,))
	if not r:
		db.execute('INSERT INTO leaderboards VALUES(1, %s)', (name,))

def make_entries(total=1000000):
	to = 0
	rows = []
	for uid in range(1, total + 1):
		if len(rows) == 1000: 
			rows.append('(%d, %d, %d)' % (uid, 1, randint(0, 10000)))
			db.execute('INSERT INTO entries VALUES ' + ', '.join(rows))
			rows = []
		else:
			rows.append('(%d, %d, %d)' % (uid, 1, randint(0, 10000)))
	db.execute('INSERT INTO entries VALUES ' + ', '.join(rows))


if __name__ == '__main__':
	create_lb()
	make_entries()