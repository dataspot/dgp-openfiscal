from akariv/dgp-app:latest

USER root

# Stuff to be done as root?

USER etl

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY configuration.json dags/
COPY logo.png ui/dist/ui/en/assets/logo.png
COPY favicons/* ui/dist/ui/
COPY os_entrypoint.sh .
COPY server_extra.py .

COPY taxonomies taxonomies

ENTRYPOINT [ "/app/os_entrypoint.sh" ]