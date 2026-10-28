from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent

for parent in (SCRIPT_DIR, *SCRIPT_DIR.parents):
    if (parent / "metadata_utils.py").is_file():
        sys.path.insert(0, str(parent))
        break
else:
    raise RuntimeError(
        "error."
    )

from metadata_utils import run_metadata_job

TOPIC = "Global Dementia Observatory"

SEARCH_TERMS = [
    "dementia",
    "Alzheimer",
    "cognitive decline",
    "Cholesterol, mean total",
    "Physical activity",
    "Obesity among adults",
    "Hypertension among adults",
    "Overweight among adults",
    "Alcohol, total per capita",
    "Depression, population-based prevalence",
    "Dementia national plan",
    "Dementia inclusion",
    "Dementia plans",
    "Inclusion of human rights as a guiding principle of the dementia plan",
    "Dementia: legislation",
    "Percentage of sub-national regions covered by dementia",
    "Dementia sub-national plan",
    "Dementia care",
    "Dementia facilities",
    "Dementia palliative",
    "Dementia diagnostic",
    "Dementia standards",
    "Dementia workforce",
    "Psychiatrists working in mental health sector",
    "Dementia psychosocial",
    "Dementia training",
    "Dementia: Adult day centres",
    "Dementia nongovernmental organization",
    "Pharmacists",
    "Medical doctors",
    "Nursing and midwifery personnel",
    "Dementia diagnosis",
    "Dementia assistive technology",
    "Dementia treatment",
    "Beds, hospital beds",
    "Health infrastructure",
    "Dementia research",
    "Dementia integrated",
    "Dementia implementation",
    "Dementia carer",
    "Dementia campaign",
    "Dementia-friendly",
    "Dementia routine monitoring",
    "Dementia reporting",
]


OUTPUT_FILE = SCRIPT_DIR / "GDO_indicator_metadata.csv"


def main():
    run_metadata_job(TOPIC, SEARCH_TERMS, OUTPUT_FILE)


if __name__ == "__main__":
    main()