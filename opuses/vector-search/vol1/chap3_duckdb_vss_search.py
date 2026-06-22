import duckdb
from sentence_transformers import SentenceTransformer


def main():
    con = duckdb.connect("var/vectors.duckdb")

    con.execute("INSTALL vss; LOAD vss;")
    con.execute("SET hnsw_enable_experimental_persistence = true;")

    con.execute("DROP TABLE IF EXISTS documents;")
    con.execute("""CREATE TABLE documents (
        id INTEGER PRIMARY KEY,
        content TEXT,
        embedding FLOAT[768]
    ) """)

    documents = [
        "Pythonは汎用プログラミング言語です",
        "機械学習にはPythonがよく使われます",
        "Rustはシステムプログラミング言語です",
        "ベクトル検索はAIアプリケーションの基盤技術です",
        "データベースはデータを永続化するシステムです",
    ]

    model = SentenceTransformer("intfloat/multilingual-e5-base", device="cuda")
    embeddings = model.encode(
        [f"passage: {doc}" for doc in documents], normalize_embeddings=True
    )

    for i, (doc, emb) in enumerate(zip(documents, embeddings)):
        con.execute(
            "INSERT INTO documents (id, content, embedding) VALUES (?, ?, ?)",
            (i, doc, emb.tolist()),
        )
    _count = con.execute("SELECT COUNT(*) FROM documents").fetchall()[0][0]
    print(f"Inserted {_count} documents into DuckDB")

    # Create an index on the embedding column for efficient vector search
    con.execute("""CREATE INDEX IF NOT EXISTS idx_documents_embedding
        ON documents USING hnsw (embedding) WITH (metric = 'cosine');""")

    query = "AIと機械学習の関係"
    query_embedding = model.encode(
        f"query: {query}", normalize_embeddings=True
    ).tolist()

    results = con.execute(
        """SELECT content,
        array_cosine_distance(embedding, ?::FLOAT[768]) AS distance
        FROM documents ORDER BY distance ASC LIMIT 3;""",
        [query_embedding],
    ).fetchall()

    print(f"{query=}")
    print("Top 3 results:")
    for content, distance in results:
        similarity = 1 - distance
        print(f"{similarity:.4f}: {content}")

    con.close()


if __name__ == "__main__":
    main()
