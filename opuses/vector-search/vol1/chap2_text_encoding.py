from sentence_transformers import SentenceTransformer


def main():
    model = SentenceTransformer("intfloat/multilingual-e5-base", device="cuda")

    documents = [
        "電気自動車は環境にやさしい移動手段です",
        "バッテリー駆動の新しい乗用車が増えています",
        "今日は良い天気です",
        "洗濯機は衣類を洗うための家電です",
    ]
    doc_embeddings = model.encode(
        [f"passage: {doc}" for doc in documents], normalize_embeddings=True
    )
    print(f"{doc_embeddings.shape=}")

    query = "EV"
    query_embedding = model.encode(f"query: {query}", normalize_embeddings=True)

    similarities = doc_embeddings @ query_embedding
    ranked = sorted(zip(similarities, documents), reverse=True)

    print(f"{query=}")
    for score, doc in ranked:
        print(f"{score:.4f} {doc=}")


if __name__ == "__main__":
    main()
