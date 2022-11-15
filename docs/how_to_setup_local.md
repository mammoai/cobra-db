## Setting up the database locally.

We recommend using docker-compose. First create a folder.
```bash
mkdir cobra_db_deployment
cd cobra_db_deployment
```
then create a file named `docker-compose.yml` with the following contents:
```yml
version: '3.8'
services:
    cobra-mongodb:
    image: mongo:latest
    ports:
        - "27017:27017"
    volumes:
        - db-data:/data/db
        - mongo-config:/data/configdb
    # command: [--auth]
    restart: always
```

Finally bring the database up.
```bash
docker-compose up -d
```

To access the database you can use [mongosh](https://www.mongodb.com/docs/mongodb-shell/install/) or [Compass](https://www.mongodb.com/docs/compass/master/install/). Basic knowledge of MongoDB is required. A good resource for learning is [MongoDB University](https://university.mongodb.com/).
