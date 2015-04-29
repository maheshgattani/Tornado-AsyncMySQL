import adb
import tornado.ioloop
import tornado.web
import sys
from tornado import gen
from adisp import process
import json

class Connect(object):
  adb = None

  @classmethod
  def _connect_mysql(cls, conn_type='ro'):
    prefix = '%s_' % conn_type
    if cls.adb is None:
      #print "Initializing adb"
      cls.adb = adb.Database(driver="MySQLdb", database="test_db", user="user", password="password", host="localhost")

  @classmethod
  def _get_adb(cls, conn_type='ro'):
    if cls.adb is not None:
      return cls.adb
    else:
      cls._connect_mysql(conn_type)
      return cls.adb

@process
def fetch(sql, params, retry=False, conn_type='ro', callback=None):
  adb = Connect._get_adb(conn_type)
  data = yield adb.runQuery(sql)
  if callback is not None:
    callback(data)
  
class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
      fetch("select count(*) from test_table", {}, callback = self.callback)

    def callback(self, results):
      self.write(json.dumps(results))
      self.finish()
      

application = tornado.web.Application([
    (r"/", MainHandler),
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
