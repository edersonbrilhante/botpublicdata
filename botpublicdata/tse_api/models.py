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
