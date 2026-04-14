FROM python:3.12-slim-trixie AS base
WORKDIR /app

RUN pip3 install --no-cache --upgrade pip

COPY requirements.txt .
RUN pip3 install --no-cache -r requirements.txt --target .

ENV PATH="/app/bin:${PATH}"
ENV PYTHONPATH="/app"

COPY *.py ./

FROM base AS scheduler
CMD [ "python3", "scheduler.py" ]

FROM base AS ghfe
CMD [ "python3", "ghfe.py" ]
