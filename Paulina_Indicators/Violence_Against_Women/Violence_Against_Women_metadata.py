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

TOPIC = "Violence Against Women"

SEARCH_TERMS = [
    "violence against women",
    "violence against girls",
    "intimate partner violence",
    "intimate partner violence prevalence",
    "physical violence",
    "sexual violence",
    "sexual violence by a partner",
    "physical or sexual violence",
    "non-partner sexual violence",
    "nonpartner sexual violence",
    "violence by an intimate partner",
    "violence against ever-partnered women",
    "violence against women prevalence",
    "violence during pregnancy",
    "gender-based violence",
    "spousal violence",
    "domestic violence",
    "sexual harassment",
    "violence against adolescent girls",
    "women subjected to violence",'Violence against women', 'intimate partner violence', 'non-partner sexual violence', 'IPV'
]

OUTPUT_FILE = (
    SCRIPT_DIR / "Violence_Against_Women_indicator_metadata.csv"
)


def main():
    run_metadata_job(TOPIC, SEARCH_TERMS, OUTPUT_FILE)


if __name__ == "__main__":
    main()