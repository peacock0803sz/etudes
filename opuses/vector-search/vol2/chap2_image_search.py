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


# ① クエリ画像のベクトルを生成（テキスト用の encode_text() 関数の代わりに使う）
def encode_query_image(image_path: Path) -> np.ndarray:
    """クエリ画像をEmbeddingに変換する"""
    image = Image.open(image_path).convert("RGB")
    inputs = processor(images=[image], return_tensors="pt").to(device)
    with torch.no_grad():
        image_features = model.get_image_features(**inputs).pooler_output
        image_features = image_features / image_features.norm(dim=-1, keepdim=True)
    return image_features.cpu().numpy()


image_dir = Path("var/images")
image_paths = sorted(list(image_dir.glob("*.jpg")) + list(image_dir.glob("*.png")))
print(f"Embedding対象: {len(image_paths)} 枚")
print("画像のベクトルを生成中...")
image_embeddings = encode_images(image_paths)
print(f"ベクトル群の構築完了: shape={image_embeddings.shape}")


# ② クエリ画像でベクトル群を検索する
def search_by_image(query_path: Path, top_k: int = 5) -> list[tuple[Path, float]]:
    """クエリ画像で類似画像を検索する"""
    query_embedding = encode_query_image(query_path)
    similarities = (image_embeddings @ query_embedding.T).squeeze()
    top_indices = np.argsort(similarities)[::-1][: top_k + 1]
    # クエリ画像自身を除外する
    return [
        (image_paths[i], float(similarities[i]))
        for i in top_indices
        if image_paths[i] != query_path
    ][:top_k]


# ③ 画像→画像検索の実行
query_image_path = Path("var/images/europython-keynote-01.jpg")
print(f"\nクエリ画像: {query_image_path.name}")
print("類似画像:")
results = search_by_image(query_image_path, top_k=2)
for path, score in results:
    print(f"  スコア {score:.4f}: {path.name}")
