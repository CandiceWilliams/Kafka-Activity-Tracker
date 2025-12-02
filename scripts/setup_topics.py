import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.kafka.admin import ensure_topics_exist

if __name__ == "__main__":
    print("Creating Kafka topics...")
    ensure_topics_exist()
    print("Done! Check Kafka UI to verify.")
