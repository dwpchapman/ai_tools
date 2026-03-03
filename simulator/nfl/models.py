from django.db import models

class Game(models.Model):
    id = models.IntegerField(primary_key=True)
    date = models.DateField()

    class Meta:
        db_table = "games"

class RushingStats(models.Model):
    id = models.IntegerField(primary_key=True)
    game_id = models.IntegerField()
    team = models.CharField(max_length=50)
    player = models.CharField(max_length=100)
    attempts = models.FloatField()
    yards = models.FloatField()
    average = models.FloatField()
    long = models.FloatField()
    touchdowns = models.FloatField()
    first_downs = models.FloatField()

    class Meta:
        db_table = "rushing_stats"

class ReceivingStats(models.Model):
    id = models.IntegerField(primary_key=True)
    game_id = models.IntegerField()
    team = models.CharField(max_length=50)
    player = models.CharField(max_length=100)
    receptions = models.FloatField()
    yards = models.FloatField()
    average = models.FloatField()
    long = models.FloatField()
    touchdowns = models.FloatField()
    first_downs = models.FloatField()
    targets = models.FloatField()
    yards_after_catch = models.FloatField()

    class Meta:
        db_table = "receiving_stats"

class DefenseStats(models.Model):
    id = models.AutoField(primary_key=True)
    game_id = models.IntegerField()
    team = models.CharField(max_length=50)
    player = models.CharField(max_length=100)
    rush_yards_allowed = models.FloatField()
    pass_yards_allowed = models.FloatField()
    total_yards_allowed = models.FloatField()

    class Meta:
        db_table = "defense_stats"
