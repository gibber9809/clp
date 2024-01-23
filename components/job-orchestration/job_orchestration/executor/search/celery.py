from celery import Celery
from . import celeryconfig  # type: ignore

app = Celery("search")
app.config_from_object(celeryconfig)
