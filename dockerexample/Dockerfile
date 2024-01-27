FROM python:3.10

WORKDIR /app

COPY ./requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./*.py .
COPY ./model.p .

CMD ["python", "/app.py"]
