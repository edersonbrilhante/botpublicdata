# -*- coding:utf-8 -*-
from tse_api import bots


def bot_party():
    party_bot = bots.PartyBot()
    party_bot.get_party_list()
    party_bot.get_affiliated_list()


def run():
    bot_party()
