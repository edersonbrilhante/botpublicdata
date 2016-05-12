# -*- coding:utf-8 -*-
from django.conf.urls import url
from tse_api import views

urlpatterns = [
    url(r'^index/$', views.TesteView.as_view(), name="main-view"),
]
