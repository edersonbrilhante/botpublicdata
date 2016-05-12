from __future__ import unicode_literals

import mongoengine as me

me.connect('tse_api', username='', password='')


class PartyList(me.Document):
    acronym = me.StringField(max_length=50)
    url = me.StringField(max_length=255)
