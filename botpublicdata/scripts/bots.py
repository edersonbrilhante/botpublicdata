# -*- coding:utf-8 -*-
import gc

from tse_api.bots import party


def bot_party():
    party_bot = party.PartyBot()
    party_bot.get_party_list()
    party_bot.get_affiliated_list()

    # candidate_bot = candidate.CandidateBot()
    # candidate_bot.get_candidate_list()


def run():
    gc.disable()
    bot_party()
