from datetime import date
from django.db.models import Sum, Avg
from django.http import HttpResponse
from django.shortcuts import render, get_object_or_404
from .models import RunningBack

def nfl_home(request):
    return render(request, "nfl_home.html")

def running_backs(request):
    backs = (
        RunningBack.objects
        .values("player")
        .filter(player__isnull=False)
        .filter(player__gt='')
        .distinct()
        .order_by("player")
    )

    return render(request, "running_backs.html", {"backs": backs})

def running_back_detail(request, player_name):
    games = RunningBack.objects.filter(player=player_name).select_related('game')

    # Season boundaries
    season_2024_start = date(2024, 9, 1)
    season_2024_end   = date(2025, 2, 28)

    season_2025_start = date(2025, 9, 1)
    season_2025_end   = date(2026, 2, 28)

    # Filter by season using the JOINed date
    games_2024 = games.filter(
    game__date__range=(season_2024_start, season_2024_end)
    ).order_by("game__date")

    games_2025 = games.filter(
    game__date__range=(season_2025_start, season_2025_end)
    ).order_by("game__date")

    # Aggregations
    stats_2024 = games_2024.aggregate(
        total_attempts=Sum('attempts'),
        total_yards=Sum('yards'),
        avg_attempts=Avg('attempts'),
        avg_yards=Avg('yards'),
    )
    stats_2025 = games_2025.aggregate(
        total_attempts=Sum('attempts'),
        total_yards=Sum('yards'),
        avg_attempts=Avg('attempts'),
        avg_yards=Avg('yards'),
    )

    return render(
        request,
        "running_back_detail.html",
        {
            "player_name": player_name,
            "games_2024": games_2024,
            "games_2025": games_2025,
            "stats_2024": stats_2024,
            "stats_2025": stats_2025,
        }
    )
