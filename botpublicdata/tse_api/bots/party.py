# -*- coding:utf-8 -*-
from __future__ import print_function

import csv
import datetime
from io import BytesIO, StringIO
import logging
import os
import shutil
import zipfile

from lxml import etree, html
import requests

from tornado import gen, httpclient, queues
from tornado.ioloop import IOLoop

from tse_api.base import DocBase
from tse_api.bots.connect import connect

log = logging.getLogger(__name__)


class PartyBot(object):

    def get_party_list(self):
        try:
            url = 'http://www.tse.jus.br/partidos/partidos-politicos/registrados-no-tse'
            http_client = httpclient.HTTPClient()
            response = http_client.fetch(url)
        except httpclient.HTTPError as e:
            log.error("Error: {}".format(e))
        except Exception as e:
            # Other errors are possible, such as IOError.
            log.error("Error: {}".format(e))
        else:
            http_client.close()
            try:
                filecontent = response.body.decode('utf-8')
            except UnicodeDecodeError:
                log.error('ERROR: unicode')
            else:
                parser = html.HTMLParser()
                try:
                    tree = html.parse(StringIO(filecontent), parser)
                except etree.XMLSyntaxError as details:
                    print('ERROR: parser', details.error_log)
                except Exception as e:
                    log.exception(e)
                else:
                    try:
                        db = connect('tse_api')
                        dc = DocBase(db, 'party')

                        siglas = tree.xpath('//div[@id="textoConteudo"]//*[tr]//*[a[@class="internal-link"]]')
                        for sigla in siglas:
                            # url = sigla[0].xpath('@href')[0].strip()
                            acronym = sigla.text_content().strip()
                            party = IOLoop.instance().run_sync(lambda: dc.do_find_one({'acronym': acronym}))
                            if not party:
                                party = {
                                    'acronym': acronym
                                }
                                IOLoop.instance().run_sync(lambda: dc.do_save(party))

                    except etree.XPathEvalError as details:
                        log.error('ERROR: XPath expression', details.error_log)
                    except Exception as e:
                        log.exception(e)

    @gen.coroutine
    def consumer(self, db, q):
        while True:
            if q.empty():
                return
            name, state = yield q.get()
            try:
                log.debug('consumer name: {} state: {}'.format(name, state))
                yield self.get_affiliated(db, name, state)
            finally:
                q.task_done()

    @gen.coroutine
    def producer(self, db, q):
        cursor = db.party.find()
        states_list = ['ac', 'al', 'am', 'ap', 'ba', 'ce', 'df', 'es', 'go', 'ma', 'mg', 'ms',
                       'mt', 'pa', 'pb', 'pe', 'pi', 'pr', 'rj', 'rn', 'ro', 'rr', 'rs', 'sc',
                       'se', 'sp', 'to']

        cnt = yield cursor.count()
        for _ in range(cnt):
            if(yield cursor.fetch_next):
                party_obj = cursor.next_object()
                name = '_do_'.join(party_obj['acronym'].split('do')) \
                    if party_obj['acronym'].find('do') else party_obj['acronym']
                name = name.lower()

                for state in states_list:
                    log.debug('producer name: {} state: {}'.format(name, state))
                    q.put((name, state))

    @gen.coroutine
    def _get_affiliated_list(self, db, q):
        log.info(q)
#        IOLoop.current().spawn_callback(lambda: self.consumer(q))
        yield self.producer(db, q)
        concurrency = 4
        for _ in range(concurrency):
            yield self.consumer(db, q)
        # yield q.join()

    def get_affiliated_list(self):
        q = queues.Queue()
        db = connect('tse_api')
        IOLoop.current().run_sync(lambda: self._get_affiliated_list(db, q))

    @gen.coroutine
    def get_affiliated(self, db, name, state):
        try:
            url = 'http://agencia.tse.jus.br/estatistica/sead/eleitorado/filiados/uf/filiados_{}_{}.zip'.format(name, state)

            request = requests.get(url)
            z = zipfile.ZipFile(BytesIO(request.content))
            z.extractall('/tmp/{}_{}'.format(name, state))
        except Exception as e:
            log.error('{},{},{}'.format(name, state, e))
        else:
            try:
                file_name = '/tmp/{0}_{1}/aplic/sead/lista_filiados/uf/filiados_{0}_{1}.csv'.format(name, state)

                if not os.path.isfile(file_name):
                    return

                model = 'affiliation{0}{1}'.format(name, state)
                coll = db[model]

                total = yield coll.find().count()
                total_fl = len(open(file_name, encoding="latin-1").readlines())-1
                affiliations = list()

                log.debug('folder: {}_{} - new: {} - atual: {}'.format(name, state, total_fl, total))
                if total_fl > total:
                    coll.drop()
                    with open(file_name, encoding="latin-1") as csvfile:
                        spamreader = csv.reader(csvfile, delimiter=';', quotechar='"')
                        next(spamreader)
                        for row in spamreader:
                            fil_number = row[2]
                            fil_name = row[3].encode("utf8")
                            fil_party = row[4]
                            fil_uf = row[6].encode("utf8")
                            fil_cod_city = row[7]
                            fil_city = row[8].encode("utf8")
                            fil_zone = row[9]
                            fil_section = row[10]
                            fil_date_afiliation = datetime.datetime.strptime(row[11], '%d/%m/%Y') if row[11] else None
                            fil_situaction_registry = row[12].encode("utf8")
                            fil_type_registry = row[13].encode("utf8")
                            fil_date_processing = datetime.datetime.strptime(row[14], '%d/%m/%Y') if row[14] else None
                            fil_date_disaffiliation = datetime.datetime.strptime(row[15], '%d/%m/%Y') if row[15] else None
                            fil_date_cancellation = datetime.datetime.strptime(row[16], '%d/%m/%Y') if row[16] else None
                            fil_date_regularization = datetime.datetime.strptime(row[17], '%d/%m/%Y') if row[17] else None
                            fil_reason_cancellation = row[18].encode("utf8")

                            affiliation = {
                                'number': fil_number,
                                'name': fil_name,
                                'party': fil_party,
                                'uf': fil_uf,
                                'cod_city': fil_cod_city,
                                'city': fil_city,
                                'zone': fil_zone,
                                'section': fil_section,
                                'date_afiliation': fil_date_afiliation,
                                'situaction_registry': fil_situaction_registry,
                                'type_registry': fil_type_registry,
                                'date_processing': fil_date_processing,
                                'date_disaffiliation': fil_date_disaffiliation,
                                'date_cancellation': fil_date_cancellation,
                                'date_regularization': fil_date_regularization,
                                'reason_cancellation': fil_reason_cancellation
                            }
                            affiliations.append(affiliation)
                            if len(affiliations) > 10000:
                                yield coll.insert(affiliations)
                                affiliations = list()
                        if affiliations:
                            yield coll.insert(affiliations)

            except Exception as e:
                log.error('{} - {} - {}'.format(e, name, state))
            finally:
                try:
                    shutil.rmtree('/tmp/{}_{}'.format(name, state))
                except:
                    pass
        finally:
            log.info('finalizando folder: {} {}'.format(name, state))
