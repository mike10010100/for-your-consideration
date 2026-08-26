import numpy as np
from PIL import Image

icon = Image.open("assets/icon_sparkle_star_transparent.png").convert("RGBA")
icon_resized = icon.resize((48, 48), Image.Resampling.LANCZOS)

# Create dark background sample (like Bluesky app)
bg_dark = Image.new("RGBA", (300, 70), (10, 15, 24, 255))
# Paste icon at (15, 11)
bg_dark.paste(icon_resized, (15, 11), icon_resized)

bg_dark.save("/Users/mike10010100/.gemini/antigravity-cli/brain/e13f27c9-70a5-4b6b-b6c5-9788be365474/mock_feed_item_preview.png")
print("Saved mock preview")
