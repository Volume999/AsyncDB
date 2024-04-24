# Running Local postgres in docker

```bash
# Pull the postgres image
docker pull postgres

# Run the postgres image
docker run --name postgresDB -e POSTGRES_PASSWORD=secret -d -p 5432:5432 postgres

# Connect from terminal to the db in docker
psql -h 0.0.0.0 -p 5432 -U postgres
# Then provide password ('secret' in this case)
```
