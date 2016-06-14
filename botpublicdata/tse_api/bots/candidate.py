# -*- coding:utf-8 -*-
from __future__ import print_function

import csv
import datetime
import logging
import multiprocessing
import os.path
import Queue
import shutil
from StringIO import StringIO
import zipfile

from motorengine import connect
import requests
import tornado

from tse_api import models

log = logging.getLogger(__name__)
io_loop = tornado.ioloop.IOLoop.instance()
connect("tse_api", host="localhost", port=27017, io_loop=io_loop)


class CandidateBot(object):

    def get_candidate_list(self):

        work_queue = multiprocessing.Queue()
        states_list = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS',
                       'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC',
                       'SE', 'SP', 'TO']
        states_list = ['RS']

        # for year in [1994, 1996, 1998, 2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014]:
        for year in [2014]:

            folder = 'cand_%s' % year
            url = 'http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_%s.zip' % year

            r = requests.get(url, stream=True)
            z = zipfile.ZipFile(StringIO(r.content))
            z.extractall('/tmp/candidate/%s/' % folder)

            for state in states_list:

                work_queue.put((year, state))

        num_processes = 3
        for i in range(num_processes):
            worker = Worker(work_queue)
            worker.start()

    def get_candidate(self, year, state):

        io_loop.start()

        try:

            file_name = '/tmp/candidate/cand_%s/consulta_cand_%s_%s.txt' % (year, year, state)

            if os.path.isfile(file_name):

                # affiliation_party.drop_collection()
                with open(file_name, 'rb') as csvfile:
                    spamreader = csv.reader(csvfile, delimiter=';', quotechar='"')
                    if year >= 1998 and year <= 2004:
                        self._candidate_1998(spamreader)
                    if year == 1994:
                        self._candidate_1994(spamreader)
                    if year == 2006:
                        self._candidate_2006(spamreader)
                    if year >= 2008 and year <= 2010:
                        self._candidate_2010(spamreader)
                    elif year == 2012:
                        self._candidate_2012(spamreader)
                    elif year == 2014:
                        self._candidate_2014(spamreader)

        except Exception, e:
            log.info(e)
        finally:
            io_loop.stop()
            shutil.rmtree(file_name)

    def _candidate_1994(self, spamreader):
        candidate_bot = models.CandidateBot
        candidates = list()

        for row in spamreader:

            ano_eleicao = datetime.datetime.strptime(row[2], '%Y')
            num_turno = self._valid_row(row[3])
            descricao_eleicao = self._valid_row(row[4])
            sigla_uf = self._valid_row(row[5])
            sigla_ue = self._valid_row(row[6])
            descricao_ue = self._valid_row(row[7])
            descricao_cargo = self._valid_row(row[9])
            nome_candidato = self._valid_row(row[10])
            sequencial_candidato = self._valid_row(row[11])
            numero_candidato = self._valid_row(row[12])
            nome_urna_candidato = self._valid_row(row[13])
            des_situacao_candidatura = self._valid_row(row[15])
            numero_partido = self._valid_row(row[16])
            sigla_partido = self._valid_row(row[17])
            nome_partido = self._valid_row(row[18])
            sigla_legenda = self._valid_row(row[20])
            composicao_legenda = self._valid_row(row[21])
            nome_legenda = self._valid_row(row[22])
            descricao_ocupacao = self._valid_row(row[24])

            d = datetime.datetime.strptime(row[26], '%d/%m/%y')
            data_nascimento = datetime.datetime(d.year-100, d.month, d.day)

            num_titulo_eleitoral_candidato = self._valid_row(row[27])
            descricao_sexo = self._valid_row(row[30])
            descricao_grau_instrucao = self._valid_row(row[32])
            descricao_estado_civil = self._valid_row(row[34])
            descricao_nacionalidade = self._valid_row(row[36])
            sigla_uf_nascimento = self._valid_row(row[37])
            nome_municipio_nascimento = self._valid_row(row[39])
            despesa_max_campanha = self._valid_row(row[40])
            desc_sit_tot_turno = self._valid_row(row[42])

            candidate = candidate_bot(
                ano_eleicao=ano_eleicao,
                num_turno=num_turno,
                descricao_eleicao=descricao_eleicao,
                sigla_uf=sigla_uf,
                sigla_ue=sigla_ue,
                descricao_ue=descricao_ue,
                descricao_cargo=descricao_cargo,
                nome_candidato=nome_candidato,
                sequencial_candidato=sequencial_candidato,
                numero_candidato=numero_candidato,
                nome_urna_candidato=nome_urna_candidato,
                des_situacao_candidatura=des_situacao_candidatura,
                numero_partido=numero_partido,
                sigla_partido=sigla_partido,
                nome_partido=nome_partido,
                sigla_legenda=sigla_legenda,
                composicao_legenda=composicao_legenda,
                nome_legenda=nome_legenda,
                descricao_ocupacao=descricao_ocupacao,
                data_nascimento=data_nascimento,
                num_titulo_eleitoral_candidato=num_titulo_eleitoral_candidato,
                descricao_sexo=descricao_sexo,
                descricao_grau_instrucao=descricao_grau_instrucao,
                descricao_estado_civil=descricao_estado_civil,
                descricao_nacionalidade=descricao_nacionalidade,
                sigla_uf_nascimento=sigla_uf_nascimento,
                nome_municipio_nascimento=nome_municipio_nascimento,
                despesa_max_campanha=despesa_max_campanha,
                desc_sit_tot_turno=desc_sit_tot_turno
            )
            candidates.append(candidate)
            if len(candidates) > 4000:
                self._save_mongo(candidates)
                candidates = list()
        if candidates:
            self._save_mongo(candidates)

    def _candidate_1998(self, spamreader):
        candidate_bot = models.CandidateBot
        candidates = list()

        for row in spamreader:

            ano_eleicao = datetime.datetime.strptime(row[2], '%Y')
            num_turno = self._valid_row(row[3])
            descricao_eleicao = self._valid_row(row[4])
            sigla_uf = self._valid_row(row[5])
            sigla_ue = self._valid_row(row[6])
            descricao_ue = self._valid_row(row[7])
            descricao_cargo = self._valid_row(row[9])
            nome_candidato = self._valid_row(row[10])
            sequencial_candidato = self._valid_row(row[13])
            numero_candidato = self._valid_row(row[12])
            nome_urna_candidato = self._valid_row(row[14])
            des_situacao_candidatura = self._valid_row(row[16])
            numero_partido = self._valid_row(row[17])
            sigla_partido = self._valid_row(row[18])
            nome_partido = self._valid_row(row[19])
            sigla_legenda = self._valid_row(row[21])
            composicao_legenda = self._valid_row(row[22])
            nome_legenda = self._valid_row(row[23])
            descricao_ocupacao = self._valid_row(row[25])
            data_nascimento = datetime.datetime.strptime(row[26], '%d%m%Y') if row[26] else None
            num_titulo_eleitoral_candidato = self._valid_row(row[27])
            descricao_sexo = self._valid_row(row[30])
            descricao_grau_instrucao = self._valid_row(row[32])
            descricao_estado_civil = self._valid_row(row[34])
            descricao_nacionalidade = self._valid_row(row[36])
            sigla_uf_nascimento = self._valid_row(row[37])
            nome_municipio_nascimento = self._valid_row(row[39])
            despesa_max_campanha = self._valid_row(row[40])
            desc_sit_tot_turno = self._valid_row(row[42])

            candidate = candidate_bot(
                ano_eleicao=ano_eleicao,
                num_turno=num_turno,
                descricao_eleicao=descricao_eleicao,
                sigla_uf=sigla_uf,
                sigla_ue=sigla_ue,
                descricao_ue=descricao_ue,
                descricao_cargo=descricao_cargo,
                nome_candidato=nome_candidato,
                sequencial_candidato=sequencial_candidato,
                numero_candidato=numero_candidato,
                nome_urna_candidato=nome_urna_candidato,
                des_situacao_candidatura=des_situacao_candidatura,
                numero_partido=numero_partido,
                sigla_partido=sigla_partido,
                nome_partido=nome_partido,
                sigla_legenda=sigla_legenda,
                composicao_legenda=composicao_legenda,
                nome_legenda=nome_legenda,
                descricao_ocupacao=descricao_ocupacao,
                data_nascimento=data_nascimento,
                num_titulo_eleitoral_candidato=num_titulo_eleitoral_candidato,
                descricao_sexo=descricao_sexo,
                descricao_grau_instrucao=descricao_grau_instrucao,
                descricao_estado_civil=descricao_estado_civil,
                descricao_nacionalidade=descricao_nacionalidade,
                sigla_uf_nascimento=sigla_uf_nascimento,
                nome_municipio_nascimento=nome_municipio_nascimento,
                despesa_max_campanha=despesa_max_campanha,
                desc_sit_tot_turno=desc_sit_tot_turno
            )
            candidates.append(candidate)
            if len(candidates) > 4000:
                self._save_mongo(candidates)
                candidates = list()
        if candidates:
            self._save_mongo(candidates)

    def _candidate_2006(self, spamreader):
        candidate_bot = models.CandidateBot
        candidates = list()

        for row in spamreader:

            ano_eleicao = datetime.datetime.strptime(row[2], '%Y')
            num_turno = self._valid_row(row[3])
            descricao_eleicao = self._valid_row(row[4])
            sigla_uf = self._valid_row(row[5])
            sigla_ue = self._valid_row(row[6])
            descricao_ue = self._valid_row(row[7])
            descricao_cargo = self._valid_row(row[9])
            nome_candidato = self._valid_row(row[10])
            sequencial_candidato = self._valid_row(row[13])
            numero_candidato = self._valid_row(row[12])
            nome_urna_candidato = self._valid_row(row[14])
            des_situacao_candidatura = self._valid_row(row[16])
            numero_partido = self._valid_row(row[17])
            sigla_partido = self._valid_row(row[18])
            nome_partido = self._valid_row(row[19])
            sigla_legenda = self._valid_row(row[21])
            composicao_legenda = self._valid_row(row[22])
            nome_legenda = self._valid_row(row[23])
            descricao_ocupacao = self._valid_row(row[25])
            data_nascimento = datetime.datetime.strptime(row[26], '%d/%m/%Y') if row[26] else None
            num_titulo_eleitoral_candidato = self._valid_row(row[27])
            descricao_sexo = self._valid_row(row[30])
            descricao_grau_instrucao = self._valid_row(row[32])
            descricao_estado_civil = self._valid_row(row[34])
            descricao_nacionalidade = self._valid_row(row[36])
            sigla_uf_nascimento = self._valid_row(row[37])
            nome_municipio_nascimento = self._valid_row(row[39])
            despesa_max_campanha = self._valid_row(row[40])
            desc_sit_tot_turno = self._valid_row(row[42])

            candidate = candidate_bot(
                ano_eleicao=ano_eleicao,
                num_turno=num_turno,
                descricao_eleicao=descricao_eleicao,
                sigla_uf=sigla_uf,
                sigla_ue=sigla_ue,
                descricao_ue=descricao_ue,
                descricao_cargo=descricao_cargo,
                nome_candidato=nome_candidato,
                sequencial_candidato=sequencial_candidato,
                numero_candidato=numero_candidato,
                nome_urna_candidato=nome_urna_candidato,
                des_situacao_candidatura=des_situacao_candidatura,
                numero_partido=numero_partido,
                sigla_partido=sigla_partido,
                nome_partido=nome_partido,
                sigla_legenda=sigla_legenda,
                composicao_legenda=composicao_legenda,
                nome_legenda=nome_legenda,
                descricao_ocupacao=descricao_ocupacao,
                data_nascimento=data_nascimento,
                num_titulo_eleitoral_candidato=num_titulo_eleitoral_candidato,
                descricao_sexo=descricao_sexo,
                descricao_grau_instrucao=descricao_grau_instrucao,
                descricao_estado_civil=descricao_estado_civil,
                descricao_nacionalidade=descricao_nacionalidade,
                sigla_uf_nascimento=sigla_uf_nascimento,
                nome_municipio_nascimento=nome_municipio_nascimento,
                despesa_max_campanha=despesa_max_campanha,
                desc_sit_tot_turno=desc_sit_tot_turno
            )
            candidates.append(candidate)
            if len(candidates) > 4000:
                self._save_mongo(candidates)
                candidates = list()
        if candidates:
            self._save_mongo(candidates)

    def _candidate_2010(self, spamreader):
        candidate_bot = models.CandidateBot
        candidates = list()

        for row in spamreader:

            ano_eleicao = datetime.datetime.strptime(row[2], '%Y')
            num_turno = self._valid_row(row[3])
            descricao_eleicao = self._valid_row(row[4])
            sigla_uf = self._valid_row(row[5])
            sigla_ue = self._valid_row(row[6])
            descricao_ue = self._valid_row(row[7])
            descricao_cargo = self._valid_row(row[9])
            nome_candidato = self._valid_row(row[10])
            sequencial_candidato = self._valid_row(row[13])
            numero_candidato = self._valid_row(row[12])
            nome_urna_candidato = self._valid_row(row[14])
            des_situacao_candidatura = self._valid_row(row[16])
            numero_partido = self._valid_row(row[17])
            sigla_partido = self._valid_row(row[18])
            nome_partido = self._valid_row(row[19])
            sigla_legenda = self._valid_row(row[21])
            composicao_legenda = self._valid_row(row[22])
            nome_legenda = self._valid_row(row[23])
            descricao_ocupacao = self._valid_row(row[25])

            d = datetime.datetime.strptime(row[26], '%d-%b-%y')
            data_nascimento = datetime.datetime(d.year-100, d.month, d.day)

            num_titulo_eleitoral_candidato = self._valid_row(row[27])
            descricao_sexo = self._valid_row(row[30])
            descricao_grau_instrucao = self._valid_row(row[32])
            descricao_estado_civil = self._valid_row(row[34])
            descricao_nacionalidade = self._valid_row(row[36])
            sigla_uf_nascimento = self._valid_row(row[37])
            nome_municipio_nascimento = self._valid_row(row[39])
            despesa_max_campanha = self._valid_row(row[40])
            desc_sit_tot_turno = self._valid_row(row[42])

            candidate = candidate_bot(
                ano_eleicao=ano_eleicao,
                num_turno=num_turno,
                descricao_eleicao=descricao_eleicao,
                sigla_uf=sigla_uf,
                sigla_ue=sigla_ue,
                descricao_ue=descricao_ue,
                descricao_cargo=descricao_cargo,
                nome_candidato=nome_candidato,
                sequencial_candidato=sequencial_candidato,
                numero_candidato=numero_candidato,
                nome_urna_candidato=nome_urna_candidato,
                des_situacao_candidatura=des_situacao_candidatura,
                numero_partido=numero_partido,
                sigla_partido=sigla_partido,
                nome_partido=nome_partido,
                sigla_legenda=sigla_legenda,
                composicao_legenda=composicao_legenda,
                nome_legenda=nome_legenda,
                descricao_ocupacao=descricao_ocupacao,
                data_nascimento=data_nascimento,
                num_titulo_eleitoral_candidato=num_titulo_eleitoral_candidato,
                descricao_sexo=descricao_sexo,
                descricao_grau_instrucao=descricao_grau_instrucao,
                descricao_estado_civil=descricao_estado_civil,
                descricao_nacionalidade=descricao_nacionalidade,
                sigla_uf_nascimento=sigla_uf_nascimento,
                nome_municipio_nascimento=nome_municipio_nascimento,
                despesa_max_campanha=despesa_max_campanha,
                desc_sit_tot_turno=desc_sit_tot_turno
            )
            candidates.append(candidate)
            if len(candidates) > 4000:
                self._save_mongo(candidates)
                candidates = list()
        if candidates:
            self._save_mongo(candidates)

    def _candidate_2012(self, spamreader):
        candidate_bot = models.CandidateBot
        candidates = list()

        for row in spamreader:

            ano_eleicao = datetime.datetime.strptime(row[2], '%Y')
            num_turno = self._valid_row(row[3])
            descricao_eleicao = self._valid_row(row[4])
            sigla_uf = self._valid_row(row[5])
            sigla_ue = self._valid_row(row[6])
            descricao_ue = self._valid_row(row[7])
            descricao_cargo = self._valid_row(row[9])
            nome_candidato = self._valid_row(row[10])
            sequencial_candidato = self._valid_row(row[11])
            numero_candidato = self._valid_row(row[12])
            cpf_candidato = self._valid_row(row[13])
            nome_urna_candidato = self._valid_row(row[14])
            des_situacao_candidatura = self._valid_row(row[16])
            numero_partido = self._valid_row(row[17])
            sigla_partido = self._valid_row(row[18])
            nome_partido = self._valid_row(row[19])
            sigla_legenda = self._valid_row(row[21])
            composicao_legenda = self._valid_row(row[22])
            nome_legenda = self._valid_row(row[23])
            descricao_ocupacao = self._valid_row(row[25])
            data_nascimento = datetime.datetime.strptime(row[26], '%d/%m/%Y') if row[26] else None
            num_titulo_eleitoral_candidato = self._valid_row(row[27])
            descricao_sexo = self._valid_row(row[30])
            descricao_grau_instrucao = self._valid_row(row[32])
            descricao_estado_civil = self._valid_row(row[34])
            descricao_nacionalidade = self._valid_row(row[36])
            sigla_uf_nascimento = self._valid_row(row[37])
            nome_municipio_nascimento = self._valid_row(row[39])
            despesa_max_campanha = self._valid_row(row[40])
            desc_sit_tot_turno = self._valid_row(row[42])
            nm_email = self._valid_row(row[43])

            candidate = candidate_bot(
                ano_eleicao=ano_eleicao,
                num_turno=num_turno,
                descricao_eleicao=descricao_eleicao,
                sigla_uf=sigla_uf,
                sigla_ue=sigla_ue,
                descricao_ue=descricao_ue,
                descricao_cargo=descricao_cargo,
                nome_candidato=nome_candidato,
                sequencial_candidato=sequencial_candidato,
                numero_candidato=numero_candidato,
                cpf_candidato=cpf_candidato,
                nome_urna_candidato=nome_urna_candidato,
                des_situacao_candidatura=des_situacao_candidatura,
                numero_partido=numero_partido,
                sigla_partido=sigla_partido,
                nome_partido=nome_partido,
                sigla_legenda=sigla_legenda,
                composicao_legenda=composicao_legenda,
                nome_legenda=nome_legenda,
                descricao_ocupacao=descricao_ocupacao,
                data_nascimento=data_nascimento,
                num_titulo_eleitoral_candidato=num_titulo_eleitoral_candidato,
                descricao_sexo=descricao_sexo,
                descricao_grau_instrucao=descricao_grau_instrucao,
                descricao_estado_civil=descricao_estado_civil,
                descricao_nacionalidade=descricao_nacionalidade,
                sigla_uf_nascimento=sigla_uf_nascimento,
                nome_municipio_nascimento=nome_municipio_nascimento,
                despesa_max_campanha=despesa_max_campanha,
                desc_sit_tot_turno=desc_sit_tot_turno,
                nm_email=nm_email
            )

            candidates.append(candidate)
            if len(candidates) > 4000:
                self._save_mongo(candidates)
                candidates = list()
        if candidates:
            self._save_mongo(candidates)

    def _candidate_2014(self, spamreader):
        candidate_bot = models.CandidateBot
        candidates = list()

        for row in spamreader:
            ano_eleicao = datetime.datetime.strptime(row[2], '%Y')
            num_turno = self._valid_row(row[3])
            descricao_eleicao = self._valid_row(row[4])
            sigla_uf = self._valid_row(row[5])
            sigla_ue = self._valid_row(row[6])
            descricao_ue = self._valid_row(row[7])
            descricao_cargo = self._valid_row(row[9])
            nome_candidato = self._valid_row(row[10])
            sequencial_candidato = self._valid_row(row[11])
            numero_candidato = self._valid_row(row[12])
            cpf_candidato = self._valid_row(row[13])
            nome_urna_candidato = self._valid_row(row[14])
            des_situacao_candidatura = self._valid_row(row[16])
            numero_partido = self._valid_row(row[17])
            sigla_partido = self._valid_row(row[18])
            nome_partido = self._valid_row(row[19])
            sigla_legenda = self._valid_row(row[21])
            composicao_legenda = self._valid_row(row[22])
            nome_legenda = self._valid_row(row[23])
            descricao_ocupacao = self._valid_row(row[25])
            data_nascimento = datetime.datetime.strptime(row[26], '%d/%m/%Y') if row[26] else None
            num_titulo_eleitoral_candidato = self._valid_row(row[27])
            descricao_sexo = self._valid_row(row[30])
            descricao_grau_instrucao = self._valid_row(row[32])
            descricao_estado_civil = self._valid_row(row[34])
            descricao_cor_raca = self._valid_row(row[36])
            descricao_nacionalidade = self._valid_row(row[38])
            sigla_uf_nascimento = self._valid_row(row[39])
            nome_municipio_nascimento = self._valid_row(row[41])
            despesa_max_campanha = self._valid_row(row[42])
            desc_sit_tot_turno = self._valid_row(row[44])
            nm_email = self._valid_row(row[45])

            candidate = candidate_bot(
                ano_eleicao=ano_eleicao,
                num_turno=num_turno,
                descricao_eleicao=descricao_eleicao,
                sigla_uf=sigla_uf,
                sigla_ue=sigla_ue,
                descricao_ue=descricao_ue,
                descricao_cargo=descricao_cargo,
                nome_candidato=nome_candidato,
                sequencial_candidato=sequencial_candidato,
                numero_candidato=numero_candidato,
                cpf_candidato=cpf_candidato,
                nome_urna_candidato=nome_urna_candidato,
                des_situacao_candidatura=des_situacao_candidatura,
                numero_partido=numero_partido,
                sigla_partido=sigla_partido,
                nome_partido=nome_partido,
                sigla_legenda=sigla_legenda,
                composicao_legenda=composicao_legenda,
                nome_legenda=nome_legenda,
                descricao_ocupacao=descricao_ocupacao,
                data_nascimento=data_nascimento,
                num_titulo_eleitoral_candidato=num_titulo_eleitoral_candidato,
                descricao_sexo=descricao_sexo,
                descricao_grau_instrucao=descricao_grau_instrucao,
                descricao_estado_civil=descricao_estado_civil,
                descricao_cor_raca=descricao_cor_raca,
                descricao_nacionalidade=descricao_nacionalidade,
                sigla_uf_nascimento=sigla_uf_nascimento,
                nome_municipio_nascimento=nome_municipio_nascimento,
                despesa_max_campanha=despesa_max_campanha,
                desc_sit_tot_turno=desc_sit_tot_turno,
                nm_email=nm_email
            )
            candidates.append(candidate)
            if len(candidates) > 4000:
                self._save_mongo(candidates)
                candidates = list()
        if candidates:
            self._save_mongo(candidates)

    def _save_mongo(self, candidates):
        candidate_bot = models.CandidateBot
        candidate_bot.objects.bulk_insert(candidates)

    def _valid_row(self, row):
        enc = "latin-1"
        if '#VERIFICAR BASE' not in row and row not in ('#NULO#', '#NE#', '-1') and row is not None:
            return row.decode(enc).encode("utf8")
        return None


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
                candidatebot = CandidateBot()
                candidatebot.get_candidate(name, state)
            except Queue.Empty:
                break
