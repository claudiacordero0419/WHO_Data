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

TOPIC = "Tobacco Control"

SEARCH_TERMS = [
    "tobacco",
    "tobacco control",
    "tobacco use",
    "tobacco smoking",
    "smoking prevalence",
    "smokeless tobacco",
    "smokeless",
    "second-hand smoke",
    "secondhand smoke",
    "exposure to tobacco smoke",
    "tobacco tax",
    "tobacco advertising",
    "tobacco promotion",
    "tobacco sponsorship",
    "tobacco packaging",
    "health warnings",
    "MPOWER",
    "WHO Framework Convention on Tobacco Control",
    "FCTC",
    "cigarettes",
    "electronic cigarettes",
    "e-cigarettes",
    "heated tobacco",
    "heated tobacco products",
    "cessation of tobacco use",
    "tobacco dependence",
    "ENDS",
    "tobacco-attributable mortality",
]

OUTPUT_FILE = SCRIPT_DIR / "Tobacco_Control_indicator_metadata.csv"


def main():
    run_metadata_job(TOPIC, SEARCH_TERMS, OUTPUT_FILE)


if __name__ == "__main__":
    main()