FROM apache/superset:latest

USER root

RUN /app/.venv/bin/python -m ensurepip && /app/.venv/bin/python -m pip install "psycopg2-binary" "trino[sqlalchemy]"

ENV ADMIN_USERNAME $ADMIN_USERNAME
ENV ADMIN_EMAIL $ADMIN_EMAIL
ENV ADMIN_PASSWORD $ADMIN_PASSWORD

COPY ./superset-init.sh /superset-init.sh
RUN chmod +x /superset-init.sh

COPY superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

USER superset
ENTRYPOINT [ "/superset-init.sh" ]