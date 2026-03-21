# STAGE 1
FROM python:3.10 AS Builder
RUN apt-get update && apt-get install -y gcc libxml2-dev libxslt-dev
COPY requirements.txt .
RUN python -m venv /venv
RUN /venv/bin/pip install --no-cache-dir -r requirements.txt
# STAGE 2
FROM python:3.10-slim
COPY --from=Builder /venv /venv
# nếu không dùng path thì CMD ["/venv/bin/python", "main.py"]
ENV PATH="/venv/bin:$PATH"
WORKDIR /app
COPY . .

RUN useradd --create-home appuser
USER appuser

CMD [ "python", "main.py" ]