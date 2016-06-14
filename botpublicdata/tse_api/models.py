# -*- coding:utf-8 -*-
from __future__ import unicode_literals

# import mongoengine as me
from motorengine import fields
from motorengine.document import Document


class Affiliation(Document):
    number = fields.StringField(max_length=12)
    name = fields.StringField(max_length=250)
    party = fields.StringField(max_length=50)
    uf = fields.StringField(max_length=2)
    cod_city = fields.IntField()
    city = fields.StringField(max_length=250)
    zone = fields.IntField()
    section = fields.IntField()
    date_afiliation = fields.DateTimeField()
    situaction_registry = fields.StringField(max_length=10)
    type_registry = fields.StringField(max_length=10)
    date_processing = fields.DateTimeField()
    date_disaffiliation = fields.DateTimeField()
    date_cancellation = fields.DateTimeField()
    date_regularization = fields.DateTimeField()
    reason_cancellation = fields.StringField(max_length=250)

    meta = {
        'indexes': [{
            'fields': ['$number', '$party', '$uf']
        }]
    }


class CandidateBot(Document):

    ano_eleicao = fields.DateTimeField()
    num_turno = fields.StringField(max_length=20)
    descricao_eleicao = fields.StringField(max_length=250)
    sigla_uf = fields.StringField(max_length=2)
    sigla_ue = fields.StringField(max_length=2)
    descricao_ue = fields.StringField(max_length=250)
    descricao_cargo = fields.StringField(max_length=250)
    nome_candidato = fields.StringField(max_length=250)
    sequencial_candidato = fields.StringField(max_length=250)
    numero_candidato = fields.StringField(max_length=5)
    cpf_candidato = fields.StringField(max_length=11)
    nome_urna_candidato = fields.StringField(max_length=250)
    des_situacao_candidatura = fields.StringField(max_length=250)
    numero_partido = fields.StringField(max_length=3)
    sigla_partido = fields.StringField(max_length=50)
    nome_partido = fields.StringField(max_length=250)
    sigla_legenda = fields.StringField(max_length=250)
    composicao_legenda = fields.StringField(max_length=250)
    nome_legenda = fields.StringField(max_length=250)
    descricao_ocupacao = fields.StringField(max_length=250)
    data_nascimento = fields.DateTimeField()
    num_titulo_eleitoral_candidato = fields.StringField(max_length=12)
    descricao_sexo = fields.StringField(max_length=20)
    descricao_grau_instrucao = fields.StringField(max_length=50)
    descricao_estado_civil = fields.StringField(max_length=25)
    descricao_cor_raca = fields.StringField(max_length=25)
    descricao_nacionalidade = fields.StringField(max_length=250)
    sigla_uf_nascimento = fields.StringField(max_length=2)
    nome_municipio_nascimento = fields.StringField(max_length=250)
    despesa_max_campanha = fields.StringField(max_length=10)
    desc_sit_tot_turno = fields.StringField(max_length=250)
    nm_email = fields.StringField(max_length=250)

    meta = {
        'indexes': [{
            'fields': ['$sequencial_candidato']
        }]
    }


class Party(Document):
    acronym = fields.StringField(max_length=50)
    url = fields.StringField(max_length=255)
    meta = {
        'indexes': [{
            'fields': ['$acronym']
        }]
    }


def factory_affiliation(name):

    cls = type(
        name,
        (
            Document,
        ),
        {
            'number': fields.StringField(max_length=12),
            'name': fields.StringField(max_length=250),
            'party': fields.StringField(max_length=50),
            'uf': fields.StringField(max_length=2),
            'cod_city': fields.IntField(),
            'city': fields.StringField(max_length=250),
            'zone': fields.IntField(),
            'section': fields.IntField(),
            'date_afiliation': fields.DateTimeField(),
            'situaction_registry': fields.StringField(max_length=10),
            'type_registry': fields.StringField(max_length=10),
            'date_processing': fields.DateTimeField(),
            'date_disaffiliation': fields.DateTimeField(),
            'date_cancellation': fields.DateTimeField(),
            'date_regularization': fields.DateTimeField(),
            'reason_cancellation': fields.StringField(max_length=250),
            'meta': {
                'indexes': [
                    {'fields': ['$number', '$party', '$uf', '$situaction_registry']}
                ]
            }
        }
    )
    return cls
