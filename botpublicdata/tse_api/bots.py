from __future__ import print_function

import csv
import logging
import requests
import zipfile
from StringIO import StringIO

from lxml import etree, html
import requests

from tse_api import models


log = logging.getLogger(__name__)

# connect("admin", host='mongodb://admin:senha@127.0.0.1:27017/admin')


class PartyBot(object):

    def get_party_list(self):
        try:
            url = 'http://www.tse.jus.br/partidos/partidos-politicos/registrados-no-tse'
            page = requests.get(url)
            try:
                filecontent = page.content.decode('utf-8')
            except UnicodeDecodeError:
                print('ERROR: unicode')
        except IOError as (errno, strerror):
            print('ERROR: {1}'.format(errno, strerror))
        else:
            parser = html.HTMLParser()
            try:
                tree = html.parse(StringIO(filecontent), parser)
            except etree.XMLSyntaxError, details:
                print('ERROR: parser', details.error_log)
            else:
                try:
                    siglas = tree.xpath('//div[@id="textoConteudo"]//*[tr]//*[a[@class="internal-link"]]')
                    for sigla in siglas:
                        url = sigla[0].xpath('@href')[0]
                        acronym = sigla.text_content()
                        try:
                            party_list = models.PartyList.objects.get(acronym=acronym)
                        except:
                            party_list = models.PartyList()
                        party_list.acronym = acronym
                        party_list.url = url
                        party_list.save()
                except etree.XPathEvalError, details:
                    print('ERROR: XPath expression', details.error_log)

    def get_affiliated_list(self):
        party_list = models.PartyList.objects
        states_list = ['ac', 'al', 'am', 'ap', 'ba', 'ce', 'df', 'es', 'go', 'ma', 'mg', 'ms',
                       'mt', 'pa', 'pb', 'pe', 'pi', 'pr', 'rj', 'rn', 'ro', 'rr', 'rs', 'sc',
                       'se', 'sp', 'to']
        for party in party_list:
            name = party.acronym.lower()
            for state in states_list:
                url = 'http://agencia.tse.jus.br/estatistica/sead/eleitorado/filiados/uf/filiados_%s_%s.zip' % (name, state)
                r = requests.get(url, stream=True)
                z = zipfile.ZipFile(StringIO(r.content))
                z.extractall('/tmp/%s_%s/' % (name, state))

                file_name = '/tmp/teste/aplic/sead/lista_filiados/uf/filiados_%s_%s.csv' % (name, state)
                with open(file_name, 'rb') as csvfile:
                    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
                    for row in spamreader:
                        print ', '.join(row)
# partido
#   - uf
#     - MUNICIPIO
#       -

# NUMERO DA INSCRICAO;
# NOME DO FILIADO;
# UF;
# CODIGO DO MUNICIPIO;
# NOME DO MUNICIPIO;
# ZONA ELEITORAL;
# SECAO ELEITORAL;
# DATA DA FILIACAO;
# SITUACAO DO REGISTRO;
# TIPO DO REGISTRO;
# DATA DO PROCESSAMENTO;
# DATA DA DESFILIACAO
# DATA DO CANCELAMENTO;
# DATA DA REGULARIZACAO;
# MOTIVO DO CANCELAMENTO
