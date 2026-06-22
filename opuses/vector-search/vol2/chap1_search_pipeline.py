from pathlib import Path

import numpy as np
import torch
from PIL import Image
from transformers import AutoModel, AutoProcessor

# ① モデルとプロセッサのロード
MODEL_ID = "google/siglip2-base-patch16-224"
if torch.cuda.is_available():
    device = "cuda"
elif torch.backends.mps.is_available():
    device = "mps"
else:
    device = "cpu"
print(f"使用デバイス: {device}")

processor = AutoProcessor.from_pretrained(MODEL_ID)
model = AutoModel.from_pretrained(MODEL_ID).to(device)
model.eval()


# ② 画像のベクトル生成（バッチ処理）
def encode_images(image_paths: list[Path], batch_size: int = 8) -> np.ndarray:
    """画像リストをバッチ処理でEmbeddingしベクトルに変換する"""
    all_embeddings = []
    for i in range(0, len(image_paths), batch_size):
        batch_paths = image_paths[i : i + batch_size]
        images = [Image.open(p).convert("RGB") for p in batch_paths]
        inputs = processor(images=images, return_tensors="pt").to(device)
        with torch.no_grad():
            image_features = model.get_image_features(**inputs).pooler_output
            # L2正規化（コサイン類似度計算のため）
            image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        all_embeddings.append(image_features.cpu().numpy())
        print(
            f"  処理済み: {min(i + batch_size, len(image_paths))}/{len(image_paths)} 枚"
        )
    return np.vstack(all_embeddings)


# ③ テキストのベクトル生成
def encode_text(text: str) -> np.ndarray:
    """テキストをEmbeddingしベクトルに変換する"""
    inputs = processor(
        text=[text],
        padding="max_length",
        max_length=64,
        truncation=True,
        return_tensors="pt",
    ).to(device)
    with torch.no_grad():
        text_features = model.get_text_features(**inputs).pooler_output
        text_features = text_features / text_features.norm(dim=-1, keepdim=True)
    return text_features.cpu().numpy()


# ④ ベクトル群の構築
image_dir = Path("var/images")
image_paths = sorted(list(image_dir.glob("*.jpg")) + list(image_dir.glob("*.png")))
print(f"Embedding対象: {len(image_paths)} 枚")
print("画像のベクトルを生成中...")
image_embeddings = encode_images(image_paths)
print(f"ベクトル群の構築完了: shape={image_embeddings.shape}")


# ⑤ テキストクエリによる検索
def search_by_text(query: str, top_k: int = 5) -> list[tuple[Path, float]]:
    """テキストクエリで類似画像を検索する"""
    query_embedding = encode_text(query)
    # コサイン類似度の計算（正規化済みベクトルの内積）
    similarities = (image_embeddings @ query_embedding.T).squeeze()
    # 上位k件のベクトルを取得
    top_indices = np.argsort(similarities)[::-1][:top_k]
    return [(image_paths[i], float(similarities[i])) for i in top_indices]


# ⑥ 検索の実行
queries = [
    "鳥",
    "講演",
    "海外のイベントで大舞台に立ってスピーチしている",
    "朝日が昇る空",
]
for query in queries:
    print(f"\nクエリ: 「{query}」")
    print("検索結果:")
    results = search_by_text(query, top_k=2)
    for path, score in results:
        print(f"  スコア {score:.4f}: {path.name}")
