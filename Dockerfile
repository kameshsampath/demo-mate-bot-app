FROM python:3.11-alpine

RUN apk add g++ gcc python3-dev musl-dev linux-headers \
    && pip install --upgrade pip

RUN adduser -D demo
USER demo
WORKDIR /home/demo

COPY --chown=demo:demo requirements.txt requirements.txt
RUN pip install --user -r requirements.txt

ENV PATH="/home/demo/.local/bin:${PATH}"

COPY --chown=demo:demo . .

CMD ["python","app.py"]