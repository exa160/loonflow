cd /d E:\Python\loonflow_git\loonflow

call E:\Python\loonflow-master\venv\Scripts\activate.bat

celery -A tasks worker -l info -c 4 -Q loonflow -P eventlet --logfile=logs/celery.log