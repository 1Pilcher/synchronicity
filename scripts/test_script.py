import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

def rel(*parts):
    return os.path.join(ROOT_DIR, *parts)

def main():
    print("✅ This script is running from:", BASE_DIR)
    print("📁 Market folder is:", ROOT_DIR)
    print("🧪 Test path to dictionary:", rel("Source Data", "dictionary.csv"))
