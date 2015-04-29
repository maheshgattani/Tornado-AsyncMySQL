import adb
import tornado.ioloop
import tornado.web
import sys
from tornado import gen
from adisp import process
import json

class Connect(object):
  config = None
  logger = None
  conn = {'ro': None, 'rw': None}
  cache = None
  adb = None
  cache_prefix = 'sales_rules'

  @classmethod
  def set_config(cls,config,logger):
    cls.config = config
    cls.logger = logger
    cls.adb = None
    # cls._init_cache()

  @classmethod
  def _connect_mysql(cls, conn_type='ro'):
    prefix = '%s_' % conn_type
    if cls.adb is None:
      #print "Initializing adb"
      cls.adb = adb.Database(driver="MySQLdb", database="magento_birchbox", user="mahesh", password="ohnohzahP0tahSho", host="localhost")

  @classmethod
  def _get_adb(cls, conn_type='ro'):
    if cls.adb is not None:
      return cls.adb
    else:
      cls._connect_mysql(conn_type)
      return cls.adb

@process
def fetch(sql, params, retry=False, conn_type='ro', callback=None):
  #print "should initialize adb"
  adb = Connect._get_adb(conn_type)
  #print "about to run run query"
  data = yield adb.runQuery(sql)
  if callback is not None:
    #print "callback made"
    callback(data)
  
class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
      fetch("select sleep(0.1); select count(*) from core_config_data", {}, callback = self.callback)

    def callback(self, results):
      #print "Callback received"
      #print results
      self.write(json.dumps(results))
      self.finish()
      

application = tornado.web.Application([
    (r"/", MainHandler),
])

if __name__ == "__main__":
    application.listen(8889)
    tornado.ioloop.IOLoop.instance().start()
