FROM python:3.12-slim-trixie
WORKDIR /app

RUN pip3 install --no-cache --upgrade pip

COPY requirements.txt .
RUN pip3 install --no-cache -r requirements.txt --target .

ENV PATH="/app/bin:${PATH}"
ENV PYTHONPATH="/app"

COPY handler.py serve.py runner.py redis_client.py worker.py ./

CMD [ "python3", "serve.py" ]
