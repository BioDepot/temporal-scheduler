# base docker image supporting python and poetry
from python:3.11-slim-buster

# install poetry
COPY . .
RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install

entrypoint ["poetry", "run"]
