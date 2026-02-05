from django.db.models import Sum, Avg
from django.http import HttpResponse
from django.shortcuts import render, get_object_or_404
from .models import RunningBack

def nfl_home(request):
    return HttpResponse("""
        <h1>NFL Simulator</h1>
        <a href="/nfl/running_backs">View Running Backs</a>
    """)

def running_backs(request):
    backs = RunningBack.objects.filter(
        carries__isnull = False,
        yards__isnull = False
    ).order_by('yards')   # ← descending sort

    return render(request, "running_backs.html", {"backs": backs})

def running_back_detail(request, player_name):
    games = RunningBack.objects.filter(player=player_name)
    stats = games.aggregate(
        total_carries=Sum('carries'),
        total_yards=Sum('yards'),
        avg_carries=Avg('carries'),
        avg_yards=Avg('yards')
    )
    return render(
        request,
        "running_back_detail.html",
        {
            "player_name": player_name,
            "games": games,
            "stats": stats
        }
    )
