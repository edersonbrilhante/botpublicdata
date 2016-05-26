# -*- coding:utf-8 -*-
from __future__ import unicode_literals

import logging


from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from tse_api import bots

log = logging.getLogger(__name__)


class TesteView(APIView):

    def get(self, request, *args, **kwargs):

        party_bot = bots.PartyBot()
        party_bot.get_party_list()
        return Response({}, status.HTTP_200_OK	)
