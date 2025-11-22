# Database Files

This directory contains database setup files and drivers:

- **postgresql-42.7.3.jar** - PostgreSQL JDBC driver for PySpark
- **start_db.sh** - Docker startup script for PostgreSQL container

## Usage

Start the PostgreSQL container:

```bash
cd src/database
bash start_db.sh
```

Check container status:

```bash
docker ps | grep hotel_reservations
```
