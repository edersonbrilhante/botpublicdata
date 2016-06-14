import logging

from tornado import gen


class DocBase(object):

    log = logging.getLogger(__name__)

    def __init__(self, db, collection_name):
        self.collection = db[collection_name]

    @gen.coroutine
    def do_save(self, document):
        yield self.collection.save(document)
        self.log.debug('document _id: {}'.format(repr(document['_id'])))

    @gen.coroutine
    def do_insert_bulk(self, documents):
        yield self.collection.insert(documents)
        count = yield self.collection.count()
        self.log.debug("Final count: %d" % count)

    @gen.coroutine
    def do_find_one(self, dict_search):
        document = self.collection.find_one(dict_search)
        return document

    def do_find(self, dict_search=dict()):
        document = self.collection.find(dict_search)
        return document

    def do_count(self, dict_search=dict()):
        result = yield self.collection.find(dict_search).count()
        raise gen.Return(result)

    @gen.coroutine
    def do_drop(self):
        self.collection.drop()
