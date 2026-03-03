from django.urls import path
from .views import nfl_home, running_backs, running_back_detail, run_simulation

urlpatterns = [
    path("", nfl_home, name="nfl_home"),   # homepage
    path("running_backs/", running_backs, name="running_backs"),
    path("running_backs/<str:player_name>/", running_back_detail, name="running_back_detail"),
    path("running_backs/<str:player_name>/simulate/", run_simulation, name="run_simulation"),
]
