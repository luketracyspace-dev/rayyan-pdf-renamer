
# The purpose of this code is to match PDFs of scientific articles with their respective
# metadata from Rayyan - a screening tool for systematic reviews.
# It reads each PDF, extracts its DOI or title, matches it against a Rayyan CSV export,
# and copies the PDF to an output folder with a structured filename (Author_Year_Title).
# Run the script in PowerShell from the folder containing rayyan_export.csv.

import csv
import shutil
from pathlib import Path
import re
import fitz  # PyMuPDF - reads PDF files and extracts text
from difflib import SequenceMatcher  # used for fuzzy title matching


# =============================================================================
# LOADING THE CSV
# Reads the Rayyan export CSV and returns a list of records (one dict per row).
# Keys are lowercased so column names are consistent regardless of export format.
# =============================================================================

def load_rayyan_csv(csv_path):
    records = []
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            records.append({k.lower().strip(): (v or "").strip() for k, v in row.items()})
    return records


# =============================================================================
# BUILDING THE OUTPUT FILENAME
# Constructs a clean filename from a CSV record in the format:
#   FirstAuthorSurname_Year_ShortTitle.pdf
# e.g. Ullah_2026_Effect_of_Hong_Huang_Tang_on_memory_enhancement_an.pdf
# =============================================================================

def get_first_author(authors_string):
    # Rayyan formats authors as "Surname, I. and Surname, I. and ..."
    # Split on " and " to get the first author, then take the surname before the comma
    if not authors_string:
        return "UnknownAuthor"
    first_entry = authors_string.split(" and ")[0]   # "Ullah, I."
    surname = first_entry.split(",")[0]               # "Ullah"
    return surname.strip()


def sanitize(text, max_len=50):
    # Remove characters that are illegal in filenames, replace spaces with underscores,
    # and truncate to max_len to keep filenames manageable
    text = re.sub(r"[^\w\s-]", "", text)       # remove special characters
    text = re.sub(r"\s+", "_", text.strip())    # spaces to underscores
    return text[:max_len]


def build_filename(record):
    author = get_first_author(record["authors"])
    year = record["year"][:4] if record["year"] else "XXXX"
    short_title = sanitize(record["title"])
    return f"{author}_{year}_{short_title}.pdf"


# =============================================================================
# EXTRACTING TEXT AND DOI FROM A PDF
# Only reads the first two pages — the DOI and title are almost always there.
# The DOI regex matches the standard format: 10.XXXX/anything
# =============================================================================

def extract_pdf_text(pdf_path):
    text = ""
    try:
        doc = fitz.open(pdf_path)
        for page_num in range(min(2, len(doc))):  # first two pages only
            text += doc[page_num].get_text()
        doc.close()
    except Exception as e:
        print(f"Could not read {pdf_path}: {e}")
    return text


def extract_doi(text):
    # DOIs always start with "10." followed by 4-9 digits, a slash, then the suffix
    pattern = r'\b10\.\d{4,9}/[^\s\"\'<>]+'
    match = re.search(pattern, text, re.IGNORECASE)
    return match.group(0).rstrip(".,);") if match else None


# =============================================================================
# MATCHING STRATEGY 1 — DOI LOOKUP
# Fastest and most reliable. Builds a dict from the CSV at startup so each
# lookup is O(1) rather than scanning all records for every PDF.
# =============================================================================

def build_doi_index(records):
    index = {}
    for record in records:
        doi = record["doi"].lower().strip().rstrip(".,);")
        if doi:
            index[doi] = record
    return index


def match_by_doi(doi, doi_index):
    if not doi:
        return None
    return doi_index.get(doi.lower().strip())


# =============================================================================
# MATCHING STRATEGY 2 — FUZZY TITLE MATCHING
# Used as a fallback when no DOI is found (common in older papers).
# Compares lines of PDF text against CSV titles using SequenceMatcher.
#
# Two line filters are used:
#   lines_long (> 20 chars): for single-line candidates — avoids noisy fragments
#   lines_all  (>  3 chars): for multi-line sliding windows — catches titles that
#                            are split across short consecutive lines (e.g. old
#                            journal PDFs where "RADIATION" and "AND MICROGRAVITY"
#                            appear on separate lines)
#
# Title normalisation (lowercase, strip punctuation) is pre-computed once for
# all CSV records so it isn't repeated on every comparison.
# =============================================================================

def normalise(text):
    return re.sub(r"[^\w\s]", "", text.lower())


def build_title_index(records):
    # Pre-normalise all CSV titles once at startup
    return [(record, normalise(record["title"])) for record in records if record["title"]]


def match_by_title(pdf_text, title_index, threshold=0.85):
    lines_long = [ln.strip() for ln in pdf_text.splitlines() if len(ln.strip()) > 20]
    lines_all  = [ln.strip() for ln in pdf_text.splitlines() if len(ln.strip()) > 3]
    if not lines_long and not lines_all:
        return None, 0.0

    # Build candidates: individual lines + sliding windows of 2-4 joined lines
    candidates = list(lines_long)
    for window in range(2, 5):
        for i in range(len(lines_all) - window + 1):
            candidates.append(" ".join(lines_all[i:i+window]))

    best_score = 0.0
    best_record = None

    for candidate in candidates:
        norm_candidate = normalise(candidate)
        for record, norm_title in title_index:
            score = SequenceMatcher(None, norm_candidate, norm_title).ratio()
            if score > best_score:
                best_score = score
                best_record = record

    # Only return a match if the best score meets the confidence threshold
    if best_score >= threshold:
        return best_record, best_score
    return None, best_score


# =============================================================================
# MATCH A SINGLE PDF
# Tries DOI first (fast, exact). Falls back to fuzzy title if no DOI is found.
# Returns the matching CSV record and which method was used.
# =============================================================================

def match_pdf(pdf_path, doi_index, title_index):
    text = extract_pdf_text(pdf_path)
    doi = extract_doi(text)

    record = match_by_doi(doi, doi_index)
    if record:
        return record, "doi"

    record, score = match_by_title(text, title_index)
    if record:
        return record, f"fuzzy({score:.2f})"

    return None, "unmatched"


# =============================================================================
# MAIN RUN FUNCTION
# Processes all PDFs in pdf_dir, copies matched files to output_dir with
# structured filenames, and writes two log files:
#   rename_log.csv  — full record of what was renamed and how
#   unmatched.txt   — list of PDFs that couldn't be matched
# =============================================================================

def run(pdf_dir, csv_path, output_dir, dry_run=False):
    records = load_rayyan_csv(csv_path)
    doi_index   = build_doi_index(records)
    title_index = build_title_index(records)

    pdf_files = sorted(Path(pdf_dir).glob("*.pdf"))
    print(f"Found {len(pdf_files)} PDFs, {len(records)} CSV records")

    Path(output_dir).mkdir(exist_ok=True)

    unmatched = []
    log_rows = []
    used_names = {}  # tracks output filenames to detect duplicates

    for pdf_path in pdf_files:
        record, method = match_pdf(pdf_path, doi_index, title_index)

        if record:
            new_name = build_filename(record)

            # Warn if two PDFs resolve to the same output filename
            if new_name in used_names:
                print(f"  ⚠ duplicate filename: {new_name} (already used by {used_names[new_name]})")
            used_names[new_name] = pdf_path.name

            dest = Path(output_dir) / new_name
            log_rows.append({"original": pdf_path.name, "renamed": new_name, "method": method})
            print(f"  ✓ [{method}] {pdf_path.name} → {new_name}")
            if not dry_run:
                shutil.copy2(pdf_path, dest)
        else:
            unmatched.append(pdf_path.name)
            print(f"  ✗ [unmatched] {pdf_path.name}")

    # Write list of unmatched files for manual review
    with open(Path(output_dir) / "unmatched.txt", "w") as f:
        f.write("\n".join(unmatched))

    # Write full rename log for auditing
    with open(Path(output_dir) / "rename_log.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["original", "renamed", "method"])
        writer.writeheader()
        writer.writerows(log_rows)

    print(f"\nDone. {len(log_rows)} matched, {len(unmatched)} unmatched.")


# =============================================================================
# ENTRY POINT
# Paths are resolved relative to this script file so it works regardless of
# which directory you run it from.
# Set dry_run=True to preview matches without copying any files.
# =============================================================================

if __name__ == "__main__":
    here = Path(__file__).parent
    run(
        pdf_dir=here / "articles",
        csv_path=here / "rayyan_export.csv",
        output_dir=here / "renamed_pdfs",
        dry_run=False
    )
