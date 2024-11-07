FROM python:3

RUN apt-get update && apt-get install -y \
	libpcap0.8-dev \
	libuv1-dev

RUN pip install --no-cache-dir --upgrade pip

RUN python3 -m pip install --upgrade pip setuptools wheel pip-tools

COPY scripts/requirements.in requirements.in
RUN pip-compile --output-file=requirements.txt requirements.in
RUN python3 -m pip install -r requirements.in

WORKDIR /app
RUN mkdir /app/pcaps
COPY scripts/* /app
COPY scripts/bin /app/bin

ENV TSTAT_NAME="tstat_DRv4"
ENV INTERFACE="ens3"
ENV TSTAT_PID="$TSTAT_NAME"
ENV TSTAT_DIR="/$TSTAT_NAME"
ENV VERSION="v1"

ENV TZ="Europe/Madrid"

RUN chmod +x /app/entrypoint.sh
ENTRYPOINT "/app/entrypoint.sh" $VERSION
