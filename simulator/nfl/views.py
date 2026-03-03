from urllib.parse import unquote
import time
from django.shortcuts import render
from django.views.decorators.http import require_POST
import json
import ollama
from .models import RushingStats, ReceivingStats, DefenseStats

# 32 NFL teams used for the simulation dropdown
NFL_TEAMS = [
    "Arizona Cardinals","Atlanta Falcons","Baltimore Ravens","Buffalo Bills",
    "Carolina Panthers","Chicago Bears","Cincinnati Bengals","Cleveland Browns",
    "Dallas Cowboys","Denver Broncos","Detroit Lions","Green Bay Packers",
    "Houston Texans","Indianapolis Colts","Jacksonville Jaguars","Kansas City Chiefs",
    "Las Vegas Raiders","Los Angeles Chargers","Los Angeles Rams","Miami Dolphins",
    "Minnesota Vikings","New England Patriots","New Orleans Saints","New York Giants",
    "New York Jets","Philadelphia Eagles","Pittsburgh Steelers","San Francisco 49ers",
    "Seattle Seahawks","Tampa Bay Buccaneers","Tennessee Titans","Washington Commanders",
]

def infer_opponent_team_from_last_row(rush_qs):
    if not rush_qs.exists():
        return None
    last_row = rush_qs.order_by("-id").first()

    # If DefenseStats has rows for last_row.team, assume that is the defense team
    if last_row.team and DefenseStats.objects.filter(team__iexact=last_row.team).exists():
        return last_row.team

    # If there is a game_id on the rushing row and DefenseStats uses game_id, try that
    game_id = getattr(last_row, "game_id", None)
    if game_id is not None:
        ds = DefenseStats.objects.filter(game_id=game_id).first()
        if ds:
            return ds.team

    # Fallback: try to find a defense row that contains the team string
    if last_row.team:
        ds = DefenseStats.objects.filter(team__icontains=last_row.team).first()
        if ds:
            return ds.team

    return None

def nfl_home(request):
    return render(request, "nfl_home.html")

def running_backs(request):
    # Get unique player names from rushing stats
    players = (
        RushingStats.objects
        .exclude(player__isnull=True)
        .exclude(player__exact='')
        .values_list("player", flat=True)
        .distinct()
        .order_by("player")
    )

    return render(request, "running_backs.html", {"players": players})


def running_back_detail(request, player_name):
    # Rushing aggregates
    player_name = unquote(player_name).strip()
    rush_qs = RushingStats.objects.filter(player=player_name)
    rush_games = rush_qs.count()
    rush_attempts = sum((r.attempts or 0) for r in rush_qs)
    rush_yards = sum((r.yards or 0) for r in rush_qs)
    rush_tds = sum((r.touchdowns or 0) for r in rush_qs)
    rush_long = max((r.long or 0) for r in rush_qs) if rush_qs.exists() else None
    rush_avg_per_game = (rush_yards / rush_games) if rush_games else 0
    rush_yards_per_attempt = (rush_yards / rush_attempts) if rush_attempts else 0

    # Receiving aggregates
    rec_qs = ReceivingStats.objects.filter(player=player_name)
    rec_games = rec_qs.count()
    receptions = sum((r.receptions or 0) for r in rec_qs)
    rec_yards = sum((r.yards or 0) for r in rec_qs)
    rec_tds = sum((r.touchdowns or 0) for r in rec_qs)
    rec_targets = sum((r.targets or 0) for r in rec_qs)
    rec_yac = sum((r.yards_after_catch or 0) for r in rec_qs)
    rec_yards_per_rec = (rec_yards / receptions) if receptions else 0
    # Opponent defense averages (if we can infer opponent team from last game row)
    opponent_team = None

    if rush_qs.exists():
        last_row = rush_qs.order_by("-id").first()
        opponent_team = getattr(last_row, "team", None)

    defense_averages = get_defense_averages(opponent_team) if opponent_team else None

    player_stats = {
        "player_name": player_name,
        "rush_games": rush_games,
        "rush_attempts": rush_attempts,
        "rush_yards": rush_yards,
        "rush_touchdowns": rush_tds,
        "rush_long": rush_long,
        "rush_yards_per_game": round(rush_avg_per_game, 2),
        "rush_yards_per_attempt": round(rush_yards_per_attempt, 2),
        "rec_games": rec_games,
        "receptions": receptions,
        "receiving_yards": rec_yards,
        "receiving_touchdowns": rec_tds,
        "targets": rec_targets,
        "receiving_yards_per_reception": round(rec_yards_per_rec, 2),
        "receiving_yac": rec_yac,
        "defense_averages": defense_averages,
    }
    context = {
        "player_stats": player_stats,
        "teams_list": NFL_TEAMS,
    }
    return render(request, "running_back_detail.html", context)


def get_defense_averages(team_name):
    rows = DefenseStats.objects.filter(team=team_name)
    if not rows.exists():
        return None

    games = rows.count()
    rush_allowed = sum(r.rush_yards_allowed for r in rows)
    pass_allowed = sum(r.pass_yards_allowed for r in rows)
    total_allowed = sum(r.total_yards_allowed for r in rows)

    return {
        "team": team_name,
        "games": games,
        "rush_yards_allowed": rush_allowed,
        "pass_yards_allowed": pass_allowed,
        "total_yards_allowed": total_allowed,
        "rush_yards_per_game": rush_allowed / games,
        "pass_yards_per_game": pass_allowed / games,
        "total_yards_per_game": total_allowed / games,
    }

def normalize_player_name(raw):
    return unquote(raw).strip()

@require_POST
def run_simulation(request, player_name):
    player_name = normalize_player_name(player_name)

    # Get selected opponent from form
    opponent = request.POST.get("opponent_team", "").strip()
    if not opponent:
        # fallback: try to infer opponent from last rush row
        rush_qs = RushingStats.objects.filter(player__iexact=player_name)
        opponent = infer_opponent_team_from_last_row(rush_qs) or ""

    # Gather player season aggregates (same logic as detail view)
    rush_qs = RushingStats.objects.filter(player__iexact=player_name)
    rec_qs = ReceivingStats.objects.filter(player__iexact=player_name)

    # fallback to icontains if exact match fails
    if not rush_qs.exists() and not rec_qs.exists():
        rush_qs = RushingStats.objects.filter(player__icontains=player_name)
        rec_qs = ReceivingStats.objects.filter(player__icontains=player_name)

    # compute aggregates
    rush_games = rush_qs.count()
    rush_attempts = sum((r.attempts or 0) for r in rush_qs)
    rush_yards = sum((r.yards or 0) for r in rush_qs)
    rush_tds = sum((r.touchdowns or 0) for r in rush_qs)

    receptions = sum((r.receptions or 0) for r in rec_qs)
    rec_yards = sum((r.yards or 0) for r in rec_qs)
    rec_tds = sum((r.touchdowns or 0) for r in rec_qs)

    # defense season averages for the chosen opponent (if available)
    defense_averages = get_defense_averages(opponent) if opponent else None

    # Build a compact prompt for the Ollama model
    prompt = {
        "player_name": player_name,
        "player_summary": {
            "rush_games": rush_games,
            "rush_attempts": rush_attempts,
            "rush_yards": rush_yards,
            "rush_tds": rush_tds,
            "receptions": receptions,
            "rec_yards": rec_yards,
            "rec_tds": rec_tds,
        },
        "opponent": opponent,
        "opponent_defense": defense_averages,
        "instructions": (
            "Simulate a single NFL game and return a concise predicted stat line for the player. "
            "Output JSON only with keys: rush_attempts, rush_yards, rush_tds, receptions, receiving_yards, receiving_tds, notes. "
            "Keep numbers realistic and explain any assumptions in 'notes'."
        )
    }
    prompt_text = json.dumps(prompt, default=str)
    start_time = time.time()

    try:

        resp = ollama.chat(
            # model = "glm-4.7-flash:latest",  # Works well, 169 seconds response
            # model = "gemma3:1b", # No json, but had notes.  8.5 seconds response
            # model = "deepseek-r1:1.5b",  # Way off in the numbers, 15 second response
            model = "qwen3-coder:latest",  # Works well and only 22.5 second response
            # model = "llava:7b",  #  No json and no notes.  43 second response
            # model = "llama3.3:latest",  # Works well, 385 second response
            stream = False,
            messages=[
                {
                    'role': 'user',
                    'content': prompt_text
                }
            ]
        )

    except Exception as e:
        return render(request, "simulation_error.html", {"error": str(e), "player_name": player_name})

    end_time = time.time()
    print(f"The time it took in querying the model was {end_time-start_time}")

    # --- extract model text from the non-streaming response ---
    # The client may return a dict-like object or an object with attributes.
    model_text = resp["message"]["content"]
    clean = model_text.strip()
    clean = clean.replace("```json", "").replace("```", "").strip()

    try:
        sim_result = json.loads(clean)
    except Exception:
        sim_result = {"notes": "Could not parse JSON", "raw": clean}

    # --- coerce numeric fields and ensure keys exist ---
    for k in ["rush_attempts", "rush_yards", "rush_tds", "receptions", "receiving_yards", "receiving_tds"]:
        if k in sim_result:
            try:
                sim_result[k] = float(sim_result[k])
            except Exception:
                # leave as-is if not convertible
                pass
        else:
            sim_result[k] = 0

    # --- render results and include raw_model_text for debugging ---
    context = {
        "player_name": player_name,
        "opponent": opponent,
        "defense_averages": defense_averages,
        "simulation": sim_result,
        "raw_model_text": model_text,
        "extracted_json": json.dumps(sim_result, indent=2),
    }
    return render(request, "simulation_result.html", context)
