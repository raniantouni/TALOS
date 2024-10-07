FROM python:3.8

# Creating Application Source Code Directory
RUN mkdir -p /kafka-producer

# Setting Home Directory for containers
WORKDIR /kafka-producer

# Installing python dependencies
COPY requirments.txt /kafka-producer
RUN pip install --no-cache-dir -r requirments.txt

# Copying src code to Container
COPY ./ /kafka-producer

# Application Environment variables
ENV APP_ENV development

# Exposing Ports
EXPOSE 3001

# Running Python Application
#CMD ["python", "producer_gaussian.py"]
CMD ["python", "producer.py"]
#CMD ["python", "producer_spike.py"]