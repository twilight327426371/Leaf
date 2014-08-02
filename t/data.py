from leaf import db

def create_lb(lid=2, name='unittest'):
    r = db.query_one('SELECT lid from leaderboards WHERE lid=%s', (lid,))
    if  r:
        return False
    db.execute('INSERT INTO leaderboards VALUES(%s, %s)', (lid, name,))
    return True

def make_entries(lid=2, total=1000000):
    to = 0
    rows = []
    for uid in range(1, total + 1):
        if len(rows) == 1000: 
            rows.append('(%d, %d, %d)' % (uid, lid, (total - uid)/3))
            db.execute('INSERT INTO entries VALUES ' + ', '.join(rows))
            rows = []
        else:
            rows.append('(%d, %d, %d)' % (uid, lid, (total - uid)/3))
    db.execute('INSERT INTO entries VALUES ' + ', '.join(rows))


def up(lid=2, name='unittest'):
    b = create_lb(lid, name='unittest')
    if b:
        make_entries(lid, total=100)


def down(lid):
    pass
    # db.execute('DELETE FROM entries WHERE lid=%s', (lid,))
    # db.execute('DELETE FROM leaderboards WHERE lid=%s', (lid,))