# Python 3 -> PostgreSQL (variant 14.3)
## Run tests
### Install PostgreSQL
```
brew install postgresql
```
### Install packages
```
pip3 install -r requirements.txt
cd ..
pip3 install -e lab3
```
### Launch database in Docker
```docker
docker run -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=country -d postgres:12
```
### Launch tests
```
 python3 tests/test.py
```
## Generate documentation
```
mkdir docs
cd docs
python3 -m pydoc -w  ../py2sqlm/*.py
```
### Compile pypi package
```
pip3 install twine
python setup.py sdist bdist_wheel
python3 -m twine upload --skip-existing --repository-url https://upload.pypi.org/legacy/ dist/*
```
### Install package
```
pip3 install py2sqlm-andlvovsky==1.0.0
```
