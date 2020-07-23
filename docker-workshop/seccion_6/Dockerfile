FROM python:3

COPY app /app

WORKDIR /app

RUN pip install -r requirements.txt

ENV FLASK_ENV development

ENTRYPOINT ["python"]
CMD ["app.py"]