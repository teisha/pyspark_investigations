# VSCode runs this to create the container:
# docker build -f c:\Users\laima\git\pyspark_investigations\.devcontainer\Dockerfile -t vsc-pyspark_investigations-1e4aecc11da0bcd73e42f74642895bb2 --build-arg VARIANT=3.7 c:\Users\laima\git\pyspark_investigations


# For more information, please refer to https://aka.ms/vscode-docker-python
FROM jupyter/pyspark-notebook
# ENV PYSPARK_PYTHON /usr/local/bin/python3.7
# ENV PYSPARK_DRIVER_PYTHON /usr/local/bin/python3.7
ENV PYSPARK_PYTHON /opt/conda/bin/python3.7
ENV PYSPARK_DRIVER_PYTHON /opt/conda/bin/python3.7
# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

USER root
# Install pip requirements
RUN pip freeze > requirements.txt
COPY requirements.txt /
RUN pip install -r /requirements.txt


ENV PATH=/root/.local/bin:$PATH
RUN pip install awscli --upgrade

RUN apt-get update \
    && apt-get install tree

WORKDIR /app
ADD . /app





#RUN useradd -m docker && echo "docker:docker" | chpasswd && adduser docker sudo
#USER docker
#CMD /bin/bash

#-------------------------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See https://go.microsoft.com/fwlink/?linkid=2090316 for license information.
#-------------------------------------------------------------------------------------------------------------

# To fully customize the contents of this image, use the following Dockerfile instead:
# https://github.com/microsoft/vscode-dev-containers/tree/v0.112.0/containers/javascript-node-12/.devcontainer/Dockerfile
#FROM mcr.microsoft.com/vscode/devcontainers/javascript-node:0-12

# The image referenced above includes a non-root user with sudo access. Add
# the "remoteUser" property to devcontainer.json to use it. On Linux, the container
# user's GID/UIDs will be updated to match your local UID/GID when using the image
# or dockerFile property. Update USER_UID/USER_GID below if you are using the
# dockerComposeFile property or want the image itself to start with different ID
# values. See https://aka.ms/vscode-remote/containers/non-root-user for details.
ENV APPUSER=jovyan
ARG USER_UID=1000
ARG USER_GID=$USER_UID
# # ARG USERNAME=${APPUSER}
ARG USERNAME=$APPUSER


VOLUME /home/$USERNAME/work
RUN chown 1000 /home/$USERNAME/work

# # [Optional] Update UID/GID if needed
RUN if [ "$USER_GID" != "1000" ] || [ "$USER_UID" != "1000" ]; then \
       groupmod --gid $USER_GID $USERNAME \
       && usermod --uid $USER_UID --gid $USER_GID $USERNAME \
       && chown -R $USER_UID:$USER_GID /home/$USERNAME; \
   fi
RUN echo "${USERNAME} ALL=(root) NOPASSWD:ALL" >> /etc/sudoers 
RUN chown -R $USERNAME /app 
RUN mkdir -p /home/$USERNAME/.vscode-server/extensions \
        /home/$USERNAME/.vscode-server-insiders/extensions \
    && chown -R $USERNAME \
        /home/$USERNAME/.vscode-server \
        /home/$USERNAME/.vscode-server-insiders
        
RUN fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER        

# ** [Optional] Uncomment this section to install additional packages. **
#
# ENV DEBIAN_FRONTEND=noninteractive
# RUN apt-get update \
#    && apt-get -y install --no-install-recommends <your-package-list-here> \
#    #
#    # Clean up
#    && apt-get autoremove -y \
#    && apt-get clean -y \
#    && rm -rf /var/lib/apt/lists/*
# ENV DEBIAN_FRONTEND=dialog



# Switching to a non-root user, please refer to https://aka.ms/vscode-docker-python-user-rights
# USER $USERNAME
USER $USERNAME
# RUN sudo unlink /usr/bin/python \
#     && sudo ln -s /usr/bin/python /usr/bin/python3
ENV HOME /home/$USERNAME/work

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "src/start.py"]

## Run this to start a Jupyter notebook:
##  /opt/conda/bin/jupyter-notebook /home/jovyan/work
##  Open the URL that starts with http://127.0.0.1:8888


# Python in worker has different version 2.7 than that in driver 3.7, PySpark cannot run with different minor versions.
# Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set.


# You should set the following environment variables in $SPARK_HOME/conf/spark-env.sh:

# export PYSPARK_PYTHON=/usr/bin/python3.7   --- jupyter notebook is using /opt/conda environment: /opt/conda/bin/python3.7
# export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.7

# export PYSPARK_PYTHON=/usr/local/bin/python3.7;PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.7;PYTHONUNBUFFERED=1