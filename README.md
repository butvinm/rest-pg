# Rest Pg

Simple Rest service for Postgres tables manipulations.

## API Spec

### Create table

**Request:**

`POST /api/v1/tables/{table_name}`

```json
{
    "columns": [
        {
            "name": "id",
            "type": "serial",
            "embellishment": "PRIMARY KEY"
        },
        {
            "name": "name",
            "type": "text",
            "embellishment": "NOT NULL"
        },
        {
            "name": "age",
            "type": "int",
            "embellishment": "CHECK (age > 0)"
        }
    ]
}
```

**Response:**

`201 CREATED`:
```json
{
    "columns": [
        {
            "name": "id",
            "type": "serial",
            "embellishment": "PRIMARY KEY"
        },
        {
            "name": "name",
            "type": "text",
            "embellishment": "NOT NULL"
        },
        {
            "name": "age",
            "type": "int",
            "embellishment": "CHECK (age > 0)"
        }
    ]
}
```
`400 BAD REQUEST`:
```
Bad SQL query: <reason>
```

### Get table info

**Request:**

`GET /api/v1/tables/table_info/{table_name}`

**Response:**

`200 OK`:
```json
{
    "qualified_name": "schema_name.table_name",
    "columns": {
        {
            "name": "id",
            "type": "serial",
        },
        {
            "name": "name",
            "type": "text",
        },
        {
            "name": "age",
            "type": "int",
        }
    },
    "rows": 42,
    "size": 14124124
}
```

`404 NOT FOUND`:

```No such table <table name>```

### Add rows

**Request:**

`PUT /api/v1/tables/{table_name}`
```json
{
    "rows": [
        {"name": "Alex", "age": 19},
        {"name": "John", "age": 24},
        {"name": "Alice", "age": 39}
    ]
}
```

**Response:**
`200 OK`:
```json
{
    "rows": [
        {"id": 0, "name": "Alex", "age": 19},
        {"id": 1, "name": "John", "age": 24},
        {"id": 2, "name": "Alice", "age": 39}
    ]
}
```

`404 NOT FOUND`:

```No such table <table name>```

### Remove table

**Request:**

`DELETE /api/v1/tables/{table_name}`

**Response:**
`200 OK`:

`404 NOT FOUND`:

```No such table <table name>```
