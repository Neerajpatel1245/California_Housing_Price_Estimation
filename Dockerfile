FROM python:3.12

COPY . /app

WORKDIR /app

RUN pip install -e.

EXPOSE $PORT

CMD gunicorn --workers=4 --bind 0.0.0.0:$PORT app:app