FROM condaforge/mambaforge

RUN apt-get update -q

RUN DEBIAN_FRONTEND=non-interactive apt-get install build-essential awscli jq -y -q; exit 0

RUN DEBIAN_FRONTEND=non-interactive apt-get install build-essential awscli jq -y -q --fix-missing

RUN mamba install curl boa -y