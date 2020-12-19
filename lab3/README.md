# Python 3 -> PostgreSQL (variant 14.3)
## Run tests
### Install PostgreSQL
```
brew install postgresql
```
### Install packages
```
pip3 install -r requirements.txt
pip3 install -e lab3
```
### Launch database in Docker
```docker
docker run -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=country -d postgres:12
```
