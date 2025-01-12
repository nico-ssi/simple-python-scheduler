FROM python:3.13

COPY . /app

RUN python -m pip install -r /app/src/requirements.txt

CMD python /app/src/main.py
