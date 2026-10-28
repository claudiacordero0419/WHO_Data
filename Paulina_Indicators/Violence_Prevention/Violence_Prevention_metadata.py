from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent

for parent in (SCRIPT_DIR, *SCRIPT_DIR.parents):
    if (parent / "metadata_utils.py").is_file():
        sys.path.insert(0, str(parent))
        break
else:
    raise RuntimeError("Could not locate metadata_utils.py.")

from metadata_utils import run_metadata_job

TOPIC = "Violence Prevention"

SEARCH_TERMS = [
    "violence prevention",
    "interpersonal violence",
    "violence",
    "homicide",
    "intentional injuries",
    "unintentional injuries",
    "injury mortality",
    "child maltreatment",
    "violence against children",
    "elder abuse",
    "youth violence",
    "community violence",
    "firearm violence",
    "road traffic injuries",
    "self-directed violence",
    "violence-related injuries",
    "school violence",
    "bullying",
    "violence surveillance",
    "violence prevention programme",
    "violence prevention program",
    "INSPIRE strategies",
    "child discipline",'Violence prevention', 'child protection', 'medico-legal', 'dating violence', 'life-skills', 'home-visiting', 'youth violence', 'elder abuse'
]

OUTPUT_FILE = SCRIPT_DIR / "Violence_Prevention_indicator_metadata.csv"


def main():
    run_metadata_job(TOPIC, SEARCH_TERMS, OUTPUT_FILE)


if __name__ == "__main__":
    main()
