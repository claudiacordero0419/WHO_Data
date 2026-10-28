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

TOPIC = "Healthcare Workforce Statistics"

SEARCH_TERMS = ['mental health sector', 
                    'biomedical engineers', 
                    'biomedical technicians', 
                    'qualified actively working', 
                    'nursing personnel',
                    'midwifery personnel',
                    'pharmacists',
                    'medical doctors',
                    'medical practicioners',
                    'dentists',
                    'dental assistants',
                    'environmental and occupational health',
                    'community health workers',
                    'dental prosthetic technicians',
                    'medicine professionals']

OUTPUT_FILE = SCRIPT_DIR / "HWS_indicator_metadata.csv"


def main():
    run_metadata_job(TOPIC, SEARCH_TERMS, OUTPUT_FILE)


if __name__ == "__main__":
    main()
