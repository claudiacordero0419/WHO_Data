
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

TOPIC = "Malaria"

SEARCH_TERMS = [
    "malaria",
    "malaria cases",
    "malaria incidence",
    "malaria mortality",
    "malaria deaths",
    "malaria prevention",
    "insecticide-treated nets",
    "insecticide treated nets",
    "indoor residual spraying",
    "antimalarial",
    "parasite prevalence",
    "Plasmodium falciparum",
    "Plasmodium vivax",
    "malaria diagnosis",
    "malaria treatment",
    "malaria elimination",
    "malaria surveillance",
    "malaria vaccine",
    "intermittent preventive treatment",
    "chemoprevention",
]

OUTPUT_FILE = SCRIPT_DIR / "malaria_indicator_metadata_new.csv"


def main():
    run_metadata_job(TOPIC, SEARCH_TERMS, OUTPUT_FILE)


if __name__ == "__main__":
    main()
