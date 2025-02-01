# codes

docker run -it \
  -e POSTGRES_USER="postgres" \
  -e POSTGRES_PASSWORD="postgres" \
  -e POSTGRES_DB="ny_taxi" \
  -v c:/Users/agyei/ZOOMCAMP/de_zoomcamp_2025/week_1_basics_n_setup/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgressql/data \
  -p 5433:5432 \
  postgres:17-alpine


```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

