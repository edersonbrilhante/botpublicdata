# -*- coding:utf-8 -*-
from tse_api.bots import party


def bot_party():
    party_bot = party.PartyBot()
    party_bot.get_party_list()
    party_bot.get_affiliated_list()


def run():
    bot_party()
