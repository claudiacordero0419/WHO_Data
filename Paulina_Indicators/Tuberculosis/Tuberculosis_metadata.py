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

TOPIC = "Tuberculosis"

SEARCH_TERMS = [
    "tuberculosis",
    "TB",
    "TB incidence",
    "TB mortality",
    "TB deaths",
    "RR-TB",
    "XDR-TB",
    "tuberculosis incidence",
    "tuberculosis mortality",
    "tuberculosis treatment",
    "tuberculosis notifications",
    "tuberculosis case detection",
    "multidrug-resistant tuberculosis",
    "drug-resistant tuberculosis",
    "MDR-TB",
    "rifampicin-resistant tuberculosis",
    "TB/HIV",
    "HIV-associated tuberculosis",
    "latent tuberculosis infection",
    "TB preventive treatment",
    "tuberculosis diagnosis",
    "tuberculosis prevalence",
    "tuberculosis treatment coverage",
    "tuberculosis treatment success",
    "End TB Strategy",
    "bacille Calmette-Guerin",
    "BCG vaccination",
]

OUTPUT_FILE = SCRIPT_DIR / "Tuberculosis_indicator_metadata.csv"


def main():
    run_metadata_job(TOPIC, SEARCH_TERMS, OUTPUT_FILE)


if __name__ == "__main__":
    main()
