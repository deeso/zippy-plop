from pymongo import MongoClient
import urllib
from hashlib import sha256


class MongoConnection(object):
    DB_NAME = 'syslog-events'
    GROKKED_COLLECTION = 'groked-messages'
    RAW_COLLECTION = 'syslog-messages'
    JSON_COLLECTION = 'syslog-messages-parsed'
    FMT_UP = "mongodb://{username}:{password}@{host}:{port}"
    FMT_NUP = "mongodb://{host}:{port}"

    def __init__(self, host=None, port=None, user=None,
                 password=None, db_name=None, uri=None):
        self.uri = uri
        self.host = host
        self.port = port
        self.password = None
        if password is not None:
            self.password = urllib.quote_plus(password)
        self.user = user
        self.db_name = self.DB_NAME if db_name is None else db_name

        if host is not None and port is not None:
            self.uri = self.FMT_NUP.format(**{'host': host, 'port': port})

        if self.user is not None and self.password is not None:
            self.uri = self.FMT_NUP.format(**{'host': host, 'port': port,
                                              'username': user,
                                              'password': self.password})

    def has_obj(self, mongodb_col, data):
        x = [i for i in mongodb_col.find(data).limit(1)]
        return len(x) > 0

    def insert(self, syslog_msg, json_data, check_id=True):
        x = self.insert_raw(syslog_msg, check_id=check_id)
        y = self.insert_json(json_data, check_id=check_id)
        return x, y

    def insert_raw(self, syslog_msg, check_id=True):
        sm = {'message_source': 'syslog',
              'message': syslog_msg, 'raw': syslog_msg,
              '_id': sha256(syslog_msg).hexdigest()}
        conn = MongoClient(self.uri)
        db = conn[self.db_name]
        col = db[self.RAW_COLLECTION]
        failed_check = True
        if check_id:
            failed_check = not self.has_obj(col, {'_id': sm['_id']})

        if not failed_check:
            x = [i for i in col.find({'_id': sm['_id']}).limit(1)][0]
            return False, x['_id']
        return True, col.insert_one(sm).inserted_id

    def insert_json(self, json_data, check_id=True):
        conn = MongoClient(self.uri)
        db = conn[self.db_name]
        col = db[self.JSON_COLLECTION]
        failed_check = True
        if check_id and '_id' in json_data:
            failed_check = not self.has_obj(col, {'_id': json_data['_id']})

        if not failed_check:
            x = [i for i in col.find({'_id': json_data['_id']}).limit(1)][0]
            return False, x['_id']
        return True, col.insert_one(json_data).inserted_id
