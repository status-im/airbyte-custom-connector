FROM airbyte/python-connector-base:1.1.0

COPY . ./airbyte/integration_code

RUN pip uninstall -y airbyte-cdk \
    && pip install ./airbyte/integration_code

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
