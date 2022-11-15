from django.urls import path
from Patient import views

urlpatterns = [
    path('predict', views.predict_diabetictype),
]