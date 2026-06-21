from sentence_transformers import SentenceTransformer


def main():
    text = "EVはバッテリーで駆動する乗用車です"
    model_names = [
        "intfloat/multilingual-e5-base",
        "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
    ]
    for model_name in model_names:
        model = SentenceTransformer(model_name)
        embedding = model.encode(text, normalize_embeddings=True)

        print(f"{model_name=}, {embedding.shape=}")
        print(f"{embedding[:5].round(4)=}")


if __name__ == "__main__":
    main()
