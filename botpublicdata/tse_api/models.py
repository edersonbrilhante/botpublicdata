# -*- coding:utf-8 -*-
from __future__ import unicode_literals

import mongoengine as me


class Affiliation(me.Document):
    number = me.StringField(max_length=12)
    name = me.StringField(max_length=250)
    party = me.StringField(max_length=50)
    uf = me.StringField(max_length=2)
    cod_city = me.IntField(max_length=10)
    city = me.StringField(max_length=250)
    zone = me.IntField(max_length=10)
    section = me.IntField(max_length=10)
    date_afiliation = me.DateTimeField()
    situaction_registry = me.StringField(max_length=10)
    type_registry = me.StringField(max_length=10)
    date_processing = me.DateTimeField()
    date_disaffiliation = me.DateTimeField()
    date_cancellation = me.DateTimeField()
    date_regularization = me.DateTimeField()
    reason_cancellation = me.StringField(max_length=250)

    meta = {
        'indexes': [{
            'fields': ['$number', '$party', '$uf']
        }]
    }

class Affiliation(me.Document):

    ano_eleicao = me.DateTimeField()
    num_turno = 
    descricao_eleicao = me.StringField(max_length=250)
    sigla_uf = me.StringField(max_length=2)
    sigla_ue =me.StringField(max_length=2)
    descricao_ue = me.StringField(max_length=250)
    codigo_cargo = 
    descricao_cargo =
    nome_candidato = me.StringField(max_length=250)
    sequencial_candidato = me.StringField(max_length=250)
    numero_candidato = 
    cpf_candidato = me.StringField(max_length=11)
    nome_urna_candidato = me.StringField(max_length=250)
    des_situacao_candidatura = me.StringField(max_length=250)
    sigla_partido =
    nome_partido =
    codigo_legenda =
    sigla_legenda =
    composicao_legenda =
    nome_legenda =
    codigo_ocupacao =
    descricao_ocupacao =
    data_nascimento = me.DateTimeField()
    num_titulo_eleitoral_candidato =
    idade_data_eleicao =
    codigo_sexo =
    descricao_sexo =
    cod_grau_instrucao =
    descricao_grau_instrucao =
    codigo_estado_civil =
    descricao_estado_civil =
    codigo_cor_raca =
    descricao_cor_raca =
    codigo_nacionalidade =
    descricao_nacionalidade =
    sigla_uf_nascimento =
    codigo_municipio_nascimento =
    nome_municipio_nascimento =
    despesa_max_campanha =
    desc_sit_tot_turno = me.StringField(max_length=250)
    nm_email = me.StringField(max_length=250)

    number = me.StringField(max_length=12)
    name = me.StringField(max_length=250)
    party = me.StringField(max_length=50)
    uf = me.StringField(max_length=2)
    cod_city = me.IntField(max_length=10)
    city = me.StringField(max_length=250)
    zone = me.IntField(max_length=10)
    section = me.IntField(max_length=10)
    date_afiliation = me.DateTimeField()
    situaction_registry = me.StringField(max_length=10)
    type_registry = me.StringField(max_length=10)
    date_processing = me.DateTimeField()
    date_disaffiliation = me.DateTimeField()
    date_cancellation = me.DateTimeField()
    date_regularization = me.DateTimeField()
    reason_cancellation = me.StringField(max_length=250)

    meta = {
        'indexes': [{
            'fields': ['$number', '$party', '$uf']
        }]
    }


class Party(me.Document):
    acronym = me.StringField(max_length=50)
    url = me.StringField(max_length=255)
    meta = {
        'indexes': [{
            'fields': ['$acronym']
        }]
    }


def factory_affiliation(name):

    cls = type(
        name,
        (
            me.Document,
        ),
        {
            'number': me.StringField(max_length=12),
            'name': me.StringField(max_length=250),
            'party': me.StringField(max_length=50),
            'uf': me.StringField(max_length=2),
            'cod_city': me.IntField(max_length=10),
            'city': me.StringField(max_length=250),
            'zone': me.IntField(max_length=10),
            'section': me.IntField(max_length=10),
            'date_afiliation': me.DateTimeField(),
            'situaction_registry': me.StringField(max_length=10),
            'type_registry': me.StringField(max_length=10),
            'date_processing': me.DateTimeField(),
            'date_disaffiliation': me.DateTimeField(),
            'date_cancellation': me.DateTimeField(),
            'date_regularization': me.DateTimeField(),
            'reason_cancellation': me.StringField(max_length=250),
            'meta': {
                'indexes': [
                    {'fields': ['$number', '$party', '$uf', '$situaction_registry']}
                ]
            }
        }
    )
    return cls
