from flask import Blueprint, render_template, request, jsonify
import psycopg2
from config import Config

bp = Blueprint("main", __name__)


def get_db_connection():
    conn = psycopg2.connect(
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        database=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
    )
    return conn


@bp.route("/")
def index():
    return render_template("index.html")


@bp.route("/datasets")
def get_datasets():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    )
    datasets = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return jsonify({"datasets": datasets})


@bp.route("/columns/<dataset>")
def get_columns(dataset):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s
        ORDER BY ordinal_position
        """,
        (dataset,),
    )
    columns = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return jsonify({"columns": columns})


@bp.route("/query", methods=["POST"])
def query_data():
    if not request.is_json:
        return jsonify({"success": False, "error": "Invalid JSON"}), 400

    data = request.get_json()
    dataset = data.get("dataset")
    filters = data.get("filters", [])
    limit = data.get("limit", 100)
    page = data.get("page", 1)
    offset = (page - 1) * limit

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        query = f"SELECT * FROM {dataset}"
        params = []

        if filters:
            wheres = []
            for filter in filters:
                column = filter["column"]
                operator = filter["operator"]
                value = filter["value"]

                if operator == "equals":
                    wheres.append(f"{column} = %s")
                    params.append(value)
                elif operator == "contains":
                    wheres.append(f"{column} ILIKE %s")
                    params.append(f"%{value}%")
                elif operator == "greater":
                    wheres.append(f"{column} > %s")
                    params.append(value)
                elif operator == "less":
                    wheres.append(f"{column} < %s")
                    params.append(value)

            if wheres:
                query += " WHERE " + " AND ".join(wheres)

        query += f" LIMIT {limit} OFFSET {offset}"

        cur.execute(query, params)
        rows = cur.fetchall()

        if cur.description is None:
            return jsonify({"success": False, "error": "No data returned"}), 400

        column_names = [desc[0] for desc in cur.description]

        results = []
        for row in rows:
            results.append(dict(zip(column_names, row)))

        return jsonify(
            {
                "success": True,
                "data": results,
                "columns": column_names,
                "count": len(results),
            }
        )

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    finally:
        cur.close()
        conn.close()
