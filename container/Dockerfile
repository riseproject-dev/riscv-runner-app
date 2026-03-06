FROM python:3.12-slim-trixie
WORKDIR /app

RUN pip3 install --no-cache --upgrade pip

COPY requirements.txt .
RUN pip3 install --no-cache -r requirements.txt --target .

ENV PATH="/app/bin:${PATH}"
ENV PYTHONPATH="/app"

COPY handler.py serve.py k8s.py db.py worker.py constants.py github.py ./

CMD [ "python3", "serve.py" ]
