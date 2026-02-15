from django.db import models

class Game(models.Model):
    id = models.IntegerField(primary_key=True)
    date = models.DateField()

    class Meta:
        db_table = "games"

class RunningBack(models.Model):
    id = models.IntegerField(primary_key=True)
    player = models.CharField(max_length=100)
    attempts = models.IntegerField()
    yards = models.IntegerField()
    game = models.ForeignKey(Game, on_delete=models.CASCADE, db_column='game_id')

    class Meta:
        db_table = "rushing_stats"
