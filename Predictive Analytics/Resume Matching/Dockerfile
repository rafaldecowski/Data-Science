FROM python:3.6
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 5000
CMD [ "python", "/usr/src/app/resume_match/app.py" ]