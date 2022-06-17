cd /d D:\loonflow\loonflow_server

call ..\venv\Scripts\activate.bat

celery flower -A tasks --address=0.0.0.0 --port=10003 --url_prefix=celerytask --basic_auth=admin:WYdj@8510@ --broker=redis://127.0.0.1:6379/0
