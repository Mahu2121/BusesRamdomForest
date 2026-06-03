from pathlib import Path
import joblib
import numpy as np
from skl2onnx import to_onnx

BASE_DIR = Path(__file__).resolve().parent.parent

model_path = BASE_DIR / "model" / "random_forest_aforo.pkl"

model = joblib.load(model_path)

sample = np.zeros((1, 13), dtype=np.float32)

onnx_model = to_onnx(
    model,
    sample,
    target_opset=12
)

output_path = BASE_DIR / "model" / "aforo.onnx"

with open(output_path, "wb") as f:
    f.write(onnx_model.SerializeToString())

print(f"ONNX guardado en: {output_path}")