set -e

PACKAGE=blobby

flake8 $PACKAGE
pycodestyle $PACKAGE
MYPYPATH=stubs mypy --strict $PACKAGE
#pylint $PACKAGE
pytest --cov-report=term-missing --cov=$PACKAGE
