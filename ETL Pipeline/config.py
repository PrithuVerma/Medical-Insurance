# ── Database ──────────────────────────────────────────
DB_HOST     = "localhost"
DB_PORT     = 3306
DB_USER     = "root"
DB_PASSWORD = ""
DB_NAME     = ""

# ── File Paths ────────────────────────────────────────
RAW_DIR       = "data/raw/"
PROCESSED_DIR = "data/processed/"

# ── Row Counts ────────────────────────────────────────
NUM_PATIENTS      = 750_000
NUM_HOSPITALS     = 130
NUM_POLICIES      = 300_000
NUM_CLAIMS        = 300_000
NUM_PROCEDURES    = 750_000   # avg 2.5 procedures per claim
NUM_CLAIM_REVIEWS = 50_000

# ── ENUM Values ───────────────────────────────────────
GENDERS        = ["Male", "Female", "Other"]
POLICY_TYPES   = ["Health", "Dental", "Vision", "Critical Illness"]
POLICY_STATUS  = ["Active", "Expired", "Cancelled"]
CLAIM_STATUS   = ["Pending", "Approved", "Denied"]
HOSPITAL_TYPES = ["Public", "Private", "Clinic"]
PROCEDURE_CATS = ["Surgery", "Consultation", "Diagnostic", "Therapy"]
DECISIONS      = ["Approved", "Denied", "Escalated"]
