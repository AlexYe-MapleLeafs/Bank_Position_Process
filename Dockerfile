FROM gcr.io/deeplearning-platform-release/base-cpu:latest
 
LABEL alexye <weibin.ye.leaf@hotmail.com>
 
RUN virtualenv /env -p python3.10
ENV VIRTUAL_ENV /env
ENV PATH /env/bin:$PATH

#set proxy
ENV http_proxy=http://gateway.bns:8080
ENV https_proxy=http://gateway.bns:8080 
ENV HTTP_PROXY=http://gateway.bns:8080
ENV HTTPS_PROXY=http://gateway.bns:8080
ARG PROXY=http://gateway.bns:8080

# Set the working directory inside the container
# WORKDIR /app
 
# Copy the script and requirements files into the container
COPY /scripts/ .
COPY requirements.txt requirements.txt
 
# Install the required Python packages

RUN pip config set global.trusted-host "pypi.org files.pythonhosted.org pypi.python.org"

RUN python -m pip install --proxy=${PROXY} --no-cache-dir --upgrade pip
#RUN pip install --proxy=${PROXY} --upgrade pip

RUN pip install --proxy=${PROXY} --no-cache-dir -r requirements.txt

# For using KubernetesPodOperator

RUN pip install --proxy=${PROXY} --no-cache --default-timeout=100 apache-airflow-providers-cncf-kubernetes


# Define the command to run when the container starts
# CMD ["python", "kyc_refresh_date.py"]
 
#unset PROXY
ENV http_proxy=
ENV https_proxy=
ENV HTTP_PROXY=
ENV HTTPS_PROXY=

