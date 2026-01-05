from django.db import models

class RunningBack(models.Model):
    player = models.CharField(max_length=100)
    carries = models.IntegerField()
    yards = models.IntegerField()

    class Meta:
        db_table = "rushing"   # ← tells Django to use your existing table
