#!/usr/bin/python

import MySQLdb
import MySQLdb.cursors
import time
import logging


#  Add the test table in mysql test dabtabase
# DROP TABLE IF EXISTS `users` ;
# CREATE TABLE `users` (
#   `uid` int(10) unsigned NOT NULL AUTO_INCREMENT,
#   `name` varchar(20) NOT NULL,
#   PRIMARY KEY (`uid`)
# )

_db_options = {
    'port': 3306,
    'host': 'localhost',
    'user': 'root',
    'passwd': '123456',
    'db': 'test',
    'use_unicode': True,
    'charset': 'utf8'
}

#__connect_adapter = None


def setup(host, user, password, db, max_idle=10, pool_opt=None, port=3306):
    global __connect_adapter, _max_idel
    opt = dict(
        host=host,
        user=user,
        passwd=password,
        db=db,
        port=port
    )
    _max_idel = max_idle
    _db_options.update(opt)

    if pool_opt:
        __connect_adapter = ThreadSafeConnectionPool(**pool_opt)
    else:
        logging.info('setup adpater to Connection')
        __connect_adapter = Connection()


class DBError(Exception):
    pass


class DBPoolError(DBError):
    pass


def query_one(sql, args=None):
    """ fecth only one row

    >>> setup('localhost', 'root', '123456', 'test')
    >>> query_one('select 1')[0]
    1L
    """
    try:
        return query(sql, args)[0]
    except:
        return None


def query(sql, args=None, many=None):
    con = None
    c = None

    def _all():
        try:
            result = c.fetchmany(many)
            while result:
                for row in result:
                    yield row
                result = c.fetchmany(many)
        finally:
            c and c.close()
            con and __connect_adapter.putcon(con)

    try:
        con = __connect_adapter.getcon()
        c = con.get_cursor()
        logging.debug("sql: " + sql + " args:" + str(args))
        c.execute(sql, args)
        if many and many > 0:
            return _all()
        else:
            return c.fetchall()

    except MySQLdb.Error, e:
        #logging.error("Error Qeury on %s", e.args[1])
        raise DBError(e.args[0], e.args[1])
    finally:

        many or (c and c.close())
        many or (con and __connect_adapter.putcon(con))


def execute(sql, args=None):
    """
    >>> setup('localhost', 'root', '123456', 'test')
    >>> execute('insert into users values(%s, %s)', [(1L, 'thomas'), (2L, 'animer')])
    2L
    >>> execute('delete from users')
    True

    """
    con = None
    c = None
    try:
        con = __connect_adapter.getcon()
        c = con.get_cursor()
        if type(args) is tuple:
            c.execute(sql, args)
        elif type(args) is list and type(args[0]) in [list, tuple]:
            if len(args) > 1:
                c.executemany(sql, args)
            else:
                c.execute(sql, args)
        elif args is None:
            c.execute(sql)
        if sql.lstrip()[:6].lower() == 'delete':
            return True
        return c.lastrowid
    except MySQLdb.Error, e:
        #logging.error("Error Execute on %s", e.args[1])
        raise DBError(e.args[0], e.args[1])

    finally:
        c and c.close()
        con and __connect_adapter.putcon(con)


class Connection(object):

    """ The Base MySQL Connection:
    >>> con = Connection()
    >>> c = con.get_cursor()
    >>> c.execute('select 1')
    1L
    >>> c.fetchone()[0]
    1L
    """

    def __init__(self):
        self._last_used = time.time()
        self._connect = None

    def connect(self):
        try:
            self._close()
            self._connect = MySQLdb.connect(**_db_options)
            self._connect.autocommit(True)
        except MySQLdb.Error, e:
            logging.error("Error MySQL on %s", e.args[1])

    def _close(self):

        if self._connect is not None:
            self._connect.close()
            self._connect = None
    close = _close

    def ensure_connect(self):
        if not self._connect or _max_idel < (time.time() - self._last_used):
            self.connect()
        self._last_used = time.time()

    def getcon(self):
        return self

    def putcon(self, c):
        pass

    def get_cursor(self, ctype=MySQLdb.cursors.Cursor):
        self.ensure_connect()
        return self._connect.cursor(ctype)


class BaseConnectionPool(object):

    def __init__(self, minconn, maxconn):

        self.maxconn = maxconn
        self.minconn = minconn if self.maxconn > minconn else int(self.maxconn * 0.2)

    def new_connect(self):
        return Connection()

    def putcon(self, con):
        pass

    def getcon(self):
        pass

    def close_all(self):
        pass


from Queue import Queue, Full, Empty


class ThreadSafeConnectionPool(BaseConnectionPool):

    def __init__(self, minconn=3, maxconn=10):
        self._created_conns = 0
        BaseConnectionPool.__init__(self, minconn, maxconn)
        import threading
        self._lock = threading.RLock()

        self._available_conns = []
        self._in_use_conns = []
        for i in range(self.minconn):
            self._available_conns.append(self.new_connect())

    def getcon(self):
        con = None
        first_tried = time.time()
        while True:
            self._lock.acquire()
            try:
                con = self._available_conns.pop()
                self._in_use_conns.append(con)
                break
            except:

                if self._created_conns < self.maxconn:

                    self._created_conns += 1
                    con = self.new_connect()
                    self._in_use_conns.append(con)
                    break
            finally:
                self._lock.release()

            if not con and 3 <= (time.time() - first_tried):
                raise DBPoolError("tried 3 seconds, can't load connection, maybe too many threads")

        return con

    def putcon(self, con):
        self._lock.acquire()
        if con in self._in_use_conns:
            self._in_use_conns.remove(con)
            self._available_conns.append(con)
        self._lock.release()

    def close_all(self):
    #all_conns = chain(self._available_conns, self._in_use_conns)
        for conn in self._available_conns:
            conn.close()
        for conn in self._in_use_conns:
            conn.close()
        self._available_conns = []
        self._in_use_conns = []


if __name__ == '__main__':
    import doctest
    doctest.testmod()
