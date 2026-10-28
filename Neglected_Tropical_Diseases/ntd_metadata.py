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

TOPIC = "Neglected Tropical Diseases"

SEARCH_TERMS = [
    "neglected tropical disease",
    "neglected tropical diseases",
    "NTD",
    "NTDs",
    "buruli",
    "Buruli ulcer",
    "Chagas disease",
    "dengue",
    "chikungunya",
    "dracunculiasis",
    "Guinea-worm disease",
    "echinococcosis",
    "foodborne trematode",
    "human African trypanosomiasis",
    "trypanosomiasis",
    "sleeping sickness",
    "leishmaniasis",
    "leprosy",
    "Hansen disease",
    "lymphatic filariasis",
    "onchocerciasis",
    "river blindness",
    "rabies",
    "scabies",
    "schistosomiasis",
    "soil-transmitted helminthiases",
    "trachoma",
    "taeniasis",
    "cysticercosis",
    "yaws",
    "snakebite envenoming",
    "preventive chemotherapy",
    "mass drug administration",
    "NTD elimination",
    "NTD eradication",
]

OUTPUT_FILE = SCRIPT_DIR / "NTD_indicator_metadata.csv"


def main():
    run_metadata_job(TOPIC, SEARCH_TERMS, OUTPUT_FILE)


if __name__ == "__main__":
    main()