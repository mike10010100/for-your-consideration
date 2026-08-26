import numpy as np
from PIL import Image

src_path = "/Users/mike10010100/.gemini/antigravity-cli/brain/e13f27c9-70a5-4b6b-b6c5-9788be365474/fyc_star_transparent_1787723256593.jpg"
img = Image.open(src_path).convert("RGB")
arr = np.array(img, dtype=np.float32) / 255.0

# Compute brightness / max channel
r, g, b = arr[:, :, 0], arr[:, :, 1], arr[:, :, 2]
max_c = np.maximum(np.maximum(r, g), b)

# Create smooth alpha curve:
# Black background (< 0.04) -> 0.0 alpha
# Ramp smoothly up to 1.0
alpha = np.clip((max_c - 0.03) / (1.0 - 0.03), 0.0, 1.0)
# Gamma curve on alpha to keep delicate sparkles vibrant
alpha = np.power(alpha, 0.85)

# Boost colors to prevent dark halos when unmultiplying
unmult_r = np.where(alpha > 0.01, np.clip(r / np.maximum(alpha, 0.15), 0.0, 1.0), 0.0)
unmult_g = np.where(alpha > 0.01, np.clip(g / np.maximum(alpha, 0.15), 0.0, 1.0), 0.0)
unmult_b = np.where(alpha > 0.01, np.clip(b / np.maximum(alpha, 0.15), 0.0, 1.0), 0.0)

rgba = np.zeros((arr.shape[0], arr.shape[1], 4), dtype=np.uint8)
rgba[:, :, 0] = (unmult_r * 255).astype(np.uint8)
rgba[:, :, 1] = (unmult_g * 255).astype(np.uint8)
rgba[:, :, 2] = (unmult_b * 255).astype(np.uint8)
rgba[:, :, 3] = (alpha * 255).astype(np.uint8)

out_img = Image.fromarray(rgba, mode="RGBA")
out_img.save("assets/icon_sparkle_star_transparent.png", "PNG")
out_img.save("/Users/mike10010100/.gemini/antigravity-cli/brain/e13f27c9-70a5-4b6b-b6c5-9788be365474/icon_sparkle_star_transparent.png", "PNG")
print("Saved transparent icon to assets/icon_sparkle_star_transparent.png")
