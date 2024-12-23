#!/bin/bash

# Fix broken packages
echo "Fixing broken installations..."
sudo apt --fix-broken install -y

# Update and upgrade the system
echo "Updating and upgrading the system..."
sudo apt update && sudo apt upgrade -y

# Download and install Google Chrome
echo "Downloading and installing Google Chrome..."
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb -O google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb
sudo apt --fix-broken install -y # Fix dependencies, if any

# List of Python libraries to install
libraries=(
    "selenium"
    "webdriver_manager"
    "bs4"
    "annotated-types"
    "anyio"
    "argon2-cffi"
    "argon2-cffi-bindings"
    "arrow"
    "asttokens"
    "async-lru"
    "async-timeout"
    "attrs"
    "babel"
    "beautifulsoup4"
    "bleach"
    "blis"
    "bs4"
    "catalogue"
    "certifi"
    "cffi"
    "charset-normalizer"
    "click"
    "cloudpathlib"
    "cloudpickle"
    "comm"
    "confection"
    "confluent-kafka"
    "contourpy"
    "cycler"
    "cymem"
    "Cython"
    "dask"
    "dateparser"
    "debugpy"
    "dnspython"
    "docopt"
    "editdistpy"
    "exceptiongroup"
    "fastai"
    "fastcore"
    "fastdownload"
    "fastjsonschema"
    "fastprogress"
    "filelock"
    "findspark"
    "fonttools"
    "fqdn"
    "fsspec"
    "ftfy"
    "h11"
    "happybase"
    "hdfs"
    "httpcore"
    "httpx"
    "huggingface-hub"
    "idna"
    "importlib_metadata"
    "joblib"
    "json5"
    "jsonpointer"
    "jsonschema-specifications"
    "kafka-python"
    "kafka-python-ng"
    "kiwisolver"
    "langcodes"
    "language_data"
    "locket"
    "malaya"
    "malaya-boilerplate"
    "marisa-trie"
    "markdown-it-py"
    "MarkupSafe"
    "matplotlib"
    "mdurl"
    "mpmath"
    "mrjob"
    "murmurhash"
    "neo4j"
    "nest-asyncio"
    "networkx"
    "numpy"
    "nvidia-cublas-cu12"
    "nvidia-cuda-cupti-cu12"
    "nvidia-cuda-nvrtc-cu12"
    "nvidia-cuda-runtime-cu12"
    "nvidia-cudnn-cu12"
    "nvidia-cufft-cu12"
    "nvidia-curand-cu12"
    "nvidia-cusolver-cu12"
    "nvidia-cusparse-cu12"
    "nvidia-nccl-cu12"
    "nvidia-nvjitlink-cu12"
    "nvidia-nvtx-cu12"
    "outcome"
    "overrides"
    "packaging"
    "pandas"
    "partd"
    "pillow"
    "preshed"
    "prometheus_client"
    "protobuf"
    "psutil"
    "pure_eval"
    "py4j"
    "pyarrow"
    "pycparser"
    "pydantic"
    "pydantic_core"
    "pymongo"
    "pyparsing"
    "PySastrawi"
    "PySocks"
    "pyspark"
    "python-dateutil"
    "python-dotenv"
    "python-json-logger"
    "python-louvain"
    "pytz"
    "PyYAML"
    "redis"
    "referencing"
    "regex"
    "requests"
    "rfc3339-validator"
    "rfc3986-validator"
    "rich"
    "rpds-py"
    "safetensors"
    "scikit-learn"
    "scipy"
    "seaborn"
    "selenium"
    "Send2Trash"
    "sentencepiece"
    "shellingham"
    "six"
    "smart-open"
    "sniffio"
    "sortedcontainers"
    "soupsieve"
    "spacy"
    "spacy-legacy"
    "spacy-loggers"
    "spark"
    "srsly"
    "stack-data"
    "sympy"
    "symspellpy"
    "thinc"
    "threadpoolctl"
    "thriftpy2"
    "timm"
    "tinycss2"
    "tokenizers"
    "tomli"
    "toolz"
    "torch"
    "torchvision"
    "tqdm"
    "transformers"
    "trio"
    "trio-websocket"
    "triton"
    "typer"
    "types-python-dateutil"
    "typing_extensions"
    "tzdata"
    "tzlocal"
    "Unidecode"
    "uri-template"
    "urllib3"
    "wasabi"
    "wcwidth"
    "weasel"
    "webcolors"
    "webdriver-manager"
    "websocket-client"
    "wheel"
    "wordcloud"
    "wiktionaryparser"
    "wrapt"
    "wsproto"
    "youtokentome"
    "zipp"
)

# Loop through the libraries and install each one
echo "Starting installation of Python libraries..."
for library in "${libraries[@]}"; do
    echo "Installing $library..."
    pip install "$library"
done

echo "All installations complete!"
