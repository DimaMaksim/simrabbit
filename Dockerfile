FROM registry.fozzy.lan/calcengine/flaskbasic

COPY . /app/

WORKDIR /app

RUN pip3 install -r requirements.txt
RUN ["apt-get", "update"]
#RUN ["apt-get", "-y", "install", "vim"]

CMD ["python3", "/app/app.py"]

