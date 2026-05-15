# pik-narvin-parser

Daily snapshot of 1-bedroom flat prices in PIK Narvin residential complex.
Public dashboard: https://pik.gorev.space

## Quick start

    python3.12 -m venv venv
    . venv/bin/activate
    pip install -e .[serve,test]
    pytest
    python -m bin.scan --db data/pik.db
    datasette serve data/pik.db -m metadata.yml --port 5051
