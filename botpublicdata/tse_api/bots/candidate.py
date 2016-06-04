# -*- coding:utf-8 -*-
from __future__ import print_function

import csv
import datetime
import logging
import multiprocessing
import Queue
import shutil
from StringIO import StringIO
import zipfile

from lxml import etree, html

import mongoengine as me
import requests

from tse_api import models


log = logging.getLogger(__name__)


class PartyBot(object):

    def get_affiliated_list(self):
        me.connection.connect('tse_api', username='', password='')

        work_queue = multiprocessing.Queue()
        party_list = models.Party.objects.filter()
        states_list = ['ac', 'al', 'am', 'ap', 'ba', 'ce', 'df', 'es', 'go', 'ma', 'mg', 'ms',
                       'mt', 'pa', 'pb', 'pe', 'pi', 'pr', 'rj', 'rn', 'ro', 'rr', 'rs', 'sc',
                       'se', 'sp', 'to']

        me.connection.disconnect()
        for party_obj in party_list:
            name = '_do_'.join(party_obj.acronym.split('do')) if party_obj.acronym.find('do') else party_obj.acronym
            name = name.lower()

            for state in states_list:
                work_queue.put((name, state))

        num_processes = 6
        for i in range(num_processes):
            worker = Worker(work_queue)
            worker.start()

    def get_candidate_list(self, name, state):
        me.connection.connect('tse_api', username='', password='')
        affiliation_party = models.factory_affiliation(str("Affiliation%s" % (name)))
        affiliations_obj = affiliation_party.objects(uf=state.upper())
        total = affiliations_obj.count()
        me.connection.disconnect()

        try:
            enc = "latin-1"
            # affiliation_party = models.factory_affiliation(str("Affiliation%s%s" % (name, state)))
            folder = 'cand_%s' % year
            url = 'http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_%s.zip' % year

            r = requests.get(url, stream=True)
            z = zipfile.ZipFile(StringIO(r.content))
            z.extractall('/tmp/%s/' % folder)

            states_list = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS',
                       'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC',
                       'SE', 'SP', 'TO']

            for state in states_list:
                file_name = '/tmp/%s/consulta_cand_%s_%s.txt' % (year, state)
                total_fl = len(open(file_name).readlines())

                log.info(total+1)
                log.info(total_fl)

                affiliations = list()
                if total_fl != total + 1:
                    affiliations_obj.delete()
                    # affiliation_party.drop_collection()
                    with open(file_name, 'rb') as csvfile:
                        spamreader = csv.reader(csvfile, delimiter=';', quotechar='"')
                        for row in spamreader:
                            fil_number = row[2]
                            fil_name = row[3].decode(enc).encode("utf8")
                            fil_party = row[4]
                            fil_uf = row[6]
                            fil_cod_city = row[7]
                            fil_city = row[8].decode(enc).encode("utf8")
                            fil_zone = row[9]
                            fil_section = row[10]
                            fil_date_afiliation = datetime.datetime.strptime(row[11], '%d/%m/%Y') if row[11] else None
                            fil_situaction_registry = row[12]
                            fil_type_registry = row[13]
                            fil_date_processing = datetime.datetime.strptime(row[14], '%d/%m/%Y') if row[14] else None
                            fil_date_disaffiliation = datetime.datetime.strptime(row[15], '%d/%m/%Y') if row[15] else None
                            fil_date_cancellation = datetime.datetime.strptime(row[16], '%d/%m/%Y') if row[16] else None
                            fil_date_regularization = datetime.datetime.strptime(row[17], '%d/%m/%Y') if row[17] else None
                            fil_reason_cancellation = row[18].decode(enc).encode("utf8")

                            affiliation = affiliation_party(
                                number=fil_number,
                                name=fil_name,
                                party=fil_party,
                                uf=fil_uf,
                                cod_city=fil_cod_city,
                                city=fil_city,
                                zone=fil_zone,
                                section=fil_section,
                                date_afiliation=fil_date_afiliation,
                                situaction_registry=fil_situaction_registry,
                                type_registry=fil_type_registry,
                                date_processing=fil_date_processing,
                                date_disaffiliation=fil_date_disaffiliation,
                                date_cancellation=fil_date_cancellation,
                                date_regularization=fil_date_regularization,
                                reason_cancellation=fil_reason_cancellation
                            )
                            affiliations.append(affiliation)
                            if len(affiliations) > 4000:
                                me.connection.connect('tse_api', username='', password='')
                                affiliation_party.objects.insert(affiliations)
                                me.connection.disconnect()
                                affiliations = list()
                        me.connection.connect('tse_api', username='', password='')
                        affiliation_party.objects.insert(affiliations)
                        me.connection.disconnect()

        except Exception, e:
            log.info(e)

        finally:
            shutil.rmtree('/tmp/%s' % folder)


class Worker(multiprocessing.Process):

    def __init__(self, work_queue):
        # base class initialization
        multiprocessing.Process.__init__(self)

        # job management stuff
        self.work_queue = work_queue
        self.kill_received = False

    def run(self):

        while not self.kill_received:

            # get a task
            try:
                name, state = self.work_queue.get_nowait()
                partybot = PartyBot()
                partybot.get_affiliated(name, state)
            except Queue.Empty:
                break
