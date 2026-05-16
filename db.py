"""
db.py — MySQL database layer for AML-TMS
Connects to Railway MySQL (or any MySQL instance).
Connection string from environment variable DATABASE_URL or individual vars.
"""
import os, json, re
from datetime import datetime

# ── Connection ────────────────────────────────────────────────────────────────
_conn = None

def get_conn():
    """Return a live MySQL connection, reconnecting if needed."""
    global _conn
    try:
        if _conn is not None:
            _conn.ping(reconnect=True)
            return _conn
    except Exception:
        _conn = None

    import pymysql
    # Railway provides DATABASE_URL=mysql://user:pass@host:port/dbname
    url = os.environ.get("DATABASE_URL", "")
    if url:
        m = re.match(r"mysql(?:\+pymysql)?://([^:]+):([^@]+)@([^:/]+):?(\d+)?/(.+)", url)
        if m:
            user, password, host, port, db = m.groups()
            port = int(port or 3306)
        else:
            raise ValueError(f"Cannot parse DATABASE_URL: {url}")
    else:
        user     = os.environ.get("MYSQLUSER",     os.environ.get("DB_USER",     "root"))
        password = os.environ.get("MYSQLPASSWORD", os.environ.get("DB_PASSWORD", ""))
        host     = os.environ.get("MYSQLHOST",     os.environ.get("DB_HOST",     "localhost"))
        port     = int(os.environ.get("MYSQLPORT", os.environ.get("DB_PORT",     3306)))
        db       = os.environ.get("MYSQLDATABASE", os.environ.get("DB_NAME",     "aml_tms"))

    # Cloudflare tunnel uses port 443 with SSL
    use_ssl = (port == 443)
    ssl_params = {"ssl": {"ssl_disabled": False}} if use_ssl else {}

    _conn = pymysql.connect(
        host=host, port=port, user=user, password=password,
        database=db, charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=15,
        **ssl_params,
    )
    print(f"[DB] Connected to MySQL at {host}:{port}/{db}", flush=True)
    return _conn


def execute(sql, params=None, fetch=None):
    """Execute SQL. fetch='one'|'all'|None."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        if fetch == "one":  return cur.fetchone()
        if fetch == "all":  return cur.fetchall()
        return cur.rowcount


def executemany(sql, rows):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


# ── Schema creation ───────────────────────────────────────────────────────────
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id       VARCHAR(30)  PRIMARY KEY,
    timestamp            DATETIME,
    reference_number     VARCHAR(40),
    amount               DOUBLE,
    currency             VARCHAR(10),
    exchange_rate        DOUBLE,
    transaction_type     VARCHAR(40),
    channel              VARCHAR(40),
    channel_idx          INT,
    payment_rail         VARCHAR(20),
    txn_status           VARCHAR(20),
    settlement_date      DATE,
    posting_date         DATE,
    narration            TEXT,
    purpose_code         VARCHAR(30),
    sender_cust_id       VARCHAR(30),
    sender_account       VARCHAR(40),
    sender_acct_type     VARCHAR(30),
    entity_name          VARCHAR(80),
    sender_bank          VARCHAR(80),
    sender_branch        VARCHAR(40),
    sender_country       VARCHAR(40),
    kyc_level            VARCHAR(20),
    customer_segment     VARCHAR(30),
    tier                 VARCHAR(20),
    tier_idx             INT,
    account_age_days     INT,
    prior_sars           INT,
    bene_cust_id         VARCHAR(30),
    bene_account         VARCHAR(40),
    bene_name            VARCHAR(80),
    bene_bank            VARCHAR(80),
    bene_branch          VARCHAR(40),
    bene_country         VARCHAR(40),
    bene_type            VARCHAR(40),
    bene_first_seen      DATE,
    bene_risk_score      DOUBLE,
    bene_blacklist       TINYINT,
    new_counterparty     TINYINT,
    counterparty_degree  INT,
    jurisdiction         VARCHAR(30),
    jurisdiction_idx     INT,
    cross_border         TINYINT,
    multi_currency       TINYINT,
    dest_risk_score      DOUBLE,
    geo_location         VARCHAR(60),
    ip_address           VARCHAR(40),
    velocity_3d          DOUBLE,
    velocity_7d          DOUBLE,
    hist_fraud_rate      DOUBLE,
    peer_fraud_rate      DOUBLE,
    amount_vs_peer_pct   DOUBLE,
    behavioral_drift     DOUBLE,
    last_txn_gap         INT,
    txn_sequence         INT,
    bene_reuse_count     INT,
    device_id            VARCHAR(30),
    browser_fp           VARCHAR(40),
    os_version           VARCHAR(30),
    session_id           VARCHAR(50),
    auth_method          VARCHAR(30),
    failed_logins        INT,
    hour_of_day          INT,
    network_cluster      VARCHAR(20),
    graph_centrality     DOUBLE,
    round_dollar         TINYINT,
    is_suspicious        TINYINT,
    typology_label       VARCHAR(60),
    expected_score_range VARCHAR(30),
    source_alert_id      VARCHAR(20),
    source_alert_score   DOUBLE,
    source_case_id       VARCHAR(20),
    analyst_decision     VARCHAR(40),
    sar_filed            TINYINT,
    fraud_confirmed      TINYINT,
    fraud_loss           DOUBLE,
    recovery_amount      DOUBLE,
    disposition_reason   TEXT,
    fp_reason            VARCHAR(100),
    investigation_time   INT,
    notes                TEXT,
    ml_score             INT,
    ml_priority          VARCHAR(20),
    ml_typology          VARCHAR(60)
);

CREATE TABLE IF NOT EXISTS alerts (
    id               VARCHAR(20)  PRIMARY KEY,
    entity           VARCHAR(80)  NOT NULL,
    amount           DOUBLE,
    score            INT,
    priority         VARCHAR(20),
    typology         VARCHAR(60),
    channel          VARCHAR(40),
    timestamp        DATETIME,
    status           VARCHAR(20)  DEFAULT 'open',
    officer          VARCHAR(60),
    case_id          VARCHAR(20),
    txn_id           VARCHAR(30),
    sender_bank      VARCHAR(80),
    sender_country   VARCHAR(40),
    bene_name        VARCHAR(80),
    bene_bank        VARCHAR(80),
    bene_country     VARCHAR(40),
    bene_risk_score  DOUBLE,
    bene_blacklist   TINYINT,
    kyc_level        VARCHAR(20),
    jurisdiction     VARCHAR(30),
    payment_rail     VARCHAR(20),
    cross_border     TINYINT,
    velocity_3d      DOUBLE,
    behavioral_drift DOUBLE,
    geo_location     VARCHAR(60),
    model_scores     JSON,
    shap_data        JSON,
    notes            TEXT,
    source           VARCHAR(20)  DEFAULT 'dataset',
    created_at       DATETIME     DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status   (status),
    INDEX idx_priority (priority),
    INDEX idx_entity   (entity(40)),
    INDEX idx_case_id  (case_id),
    INDEX idx_timestamp(timestamp)
);

CREATE TABLE IF NOT EXISTS cases (
    id            VARCHAR(20)  PRIMARY KEY,
    entity        VARCHAR(80)  NOT NULL,
    alert_ids     JSON,
    alert_count   INT          DEFAULT 0,
    priority      VARCHAR(20),
    status        VARCHAR(20)  DEFAULT 'open',
    officer       VARCHAR(60),
    opened        DATETIME,
    sar_due       DATETIME,
    typology      VARCHAR(60),
    narrative     LONGTEXT,
    sar_status    VARCHAR(20),
    escalated_by  VARCHAR(60),
    escalated_at  DATETIME,
    manual_sar    TINYINT      DEFAULT 0,
    created_at    DATETIME     DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status   (status),
    INDEX idx_priority (priority),
    INDEX idx_entity   (entity(40))
);

CREATE TABLE IF NOT EXISTS sar_records (
    id            VARCHAR(20)  PRIMARY KEY,
    case_id       VARCHAR(20),
    alert_id      VARCHAR(20),
    entity        VARCHAR(80),
    filing_officer VARCHAR(60),
    narrative     LONGTEXT,
    status        VARCHAR(20)  DEFAULT 'draft',
    filed_at      DATETIME,
    fincen_ref    VARCHAR(40),
    documents     JSON,
    created_at    DATETIME     DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_case_id (case_id),
    INDEX idx_status  (status)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id         BIGINT       AUTO_INCREMENT PRIMARY KEY,
    ts         DATETIME     DEFAULT CURRENT_TIMESTAMP,
    user_name  VARCHAR(60),
    action     VARCHAR(60),
    target     VARCHAR(40),
    detail     TEXT,
    ip_address VARCHAR(40),
    INDEX idx_ts     (ts),
    INDEX idx_target (target),
    INDEX idx_action (action)
);

CREATE TABLE IF NOT EXISTS users (
    id            INT          AUTO_INCREMENT PRIMARY KEY,
    username      VARCHAR(40)  UNIQUE NOT NULL,
    display_name  VARCHAR(80),
    role          VARCHAR(20)  DEFAULT 'analyst',
    department    VARCHAR(40),
    password_hash VARCHAR(100),
    active        TINYINT      DEFAULT 1,
    created_at    DATETIME     DEFAULT CURRENT_TIMESTAMP,
    last_login    DATETIME,
    INDEX idx_username (username)
);

CREATE TABLE IF NOT EXISTS notifications (
    id         BIGINT       AUTO_INCREMENT PRIMARY KEY,
    ts         DATETIME     DEFAULT CURRENT_TIMESTAMP,
    type       VARCHAR(30),
    title      VARCHAR(100),
    message    TEXT,
    target_id  VARCHAR(20),
    severity   VARCHAR(20),
    read_by    JSON,
    INDEX idx_ts   (ts),
    INDEX idx_type (type)
);

CREATE TABLE IF NOT EXISTS app_config (
    key_name   VARCHAR(60)  PRIMARY KEY,
    value      TEXT,
    updated_at DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
"""


def create_schema():
    """Create all tables if they don't exist."""
    conn = get_conn()
    with conn.cursor() as cur:
        for stmt in SCHEMA_SQL.strip().split(";\n\n"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
    print("[DB] Schema ready", flush=True)


# ── Seed data helpers ─────────────────────────────────────────────────────────
def is_seeded():
    """Check if data has already been loaded."""
    row = execute("SELECT COUNT(*) as n FROM transactions", fetch="one")
    return (row or {}).get("n", 0) > 0


def seed_transactions(raw_transactions):
    """Bulk-insert all 1,000 transactions from the dataset."""
    def safe_date(v):
        if not v or str(v) in ('nan','None',''): return None
        try: return str(v)[:10]
        except: return None
    def safe_float(v):
        try: return float(v) if v not in (None,'','nan') else None
        except: return None
    def safe_int(v):
        try: return int(float(v)) if v not in (None,'','nan') else None
        except: return None
    def safe_str(v, maxlen=None):
        s = str(v) if v not in (None,'nan') else ''
        return s[:maxlen] if maxlen else s

    rows = []
    for t in raw_transactions:
        rows.append((
            safe_str(t.get('transaction_id'), 30),
            safe_str(t.get('timestamp'), 19) or None,
            safe_str(t.get('reference_number'), 40),
            safe_float(t.get('amount')),
            safe_str(t.get('currency'), 10),
            safe_float(t.get('exchange_rate')),
            safe_str(t.get('transaction_type'), 40),
            safe_str(t.get('channel'), 40),
            safe_int(t.get('channel_idx')),
            safe_str(t.get('payment_rail'), 20),
            safe_str(t.get('status'), 20),
            safe_date(t.get('settlement_date')),
            safe_date(t.get('posting_date')),
            safe_str(t.get('narration')),
            safe_str(t.get('purpose_code'), 30),
            safe_str(t.get('sender_cust_id'), 30),
            safe_str(t.get('sender_account'), 40),
            safe_str(t.get('sender_acct_type'), 30),
            safe_str(t.get('entity_name'), 80),
            safe_str(t.get('sender_bank'), 80),
            safe_str(t.get('sender_branch'), 40),
            safe_str(t.get('sender_country'), 40),
            safe_str(t.get('kyc_level'), 20),
            safe_str(t.get('customer_segment'), 30),
            safe_str(t.get('tier'), 20),
            safe_int(t.get('tier_idx')),
            safe_int(t.get('account_age_days')),
            safe_int(t.get('prior_sars')),
            safe_str(t.get('bene_cust_id'), 30),
            safe_str(t.get('bene_account'), 40),
            safe_str(t.get('bene_name'), 80),
            safe_str(t.get('bene_bank'), 80),
            safe_str(t.get('bene_branch'), 40),
            safe_str(t.get('bene_country'), 40),
            safe_str(t.get('bene_type'), 40),
            safe_date(t.get('bene_first_seen')),
            safe_float(t.get('bene_risk_score')),
            safe_int(t.get('bene_blacklist')),
            safe_int(t.get('new_counterparty')),
            safe_int(t.get('counterparty_degree')),
            safe_str(t.get('jurisdiction'), 30),
            safe_int(t.get('jurisdiction_idx')),
            safe_int(t.get('cross_border')),
            safe_int(t.get('multi_currency')),
            safe_float(t.get('dest_risk_score')),
            safe_str(t.get('geo_location'), 60),
            safe_str(t.get('ip_address'), 40),
            safe_float(t.get('velocity_3d')),
            safe_float(t.get('velocity_7d')),
            safe_float(t.get('hist_fraud_rate')),
            safe_float(t.get('peer_fraud_rate')),
            safe_float(t.get('amount_vs_peer_pct')),
            safe_float(t.get('behavioral_drift')),
            safe_int(t.get('last_txn_gap')),
            safe_int(t.get('txn_sequence')),
            safe_int(t.get('bene_reuse_count')),
            safe_str(t.get('device_id'), 30),
            safe_str(t.get('browser_fp'), 40),
            safe_str(t.get('os_version'), 30),
            safe_str(t.get('session_id'), 50),
            safe_str(t.get('auth_method'), 30),
            safe_int(t.get('failed_logins')),
            safe_int(t.get('hour_of_day')),
            safe_str(t.get('network_cluster'), 20),
            safe_float(t.get('graph_centrality')),
            safe_int(t.get('round_dollar')),
            safe_int(t.get('is_suspicious')),
            safe_str(t.get('typology_label'), 60),
            safe_str(t.get('expected_score_range'), 30),
            safe_str(t.get('source_alert_id'), 20),
            safe_float(t.get('source_alert_score')),
            safe_str(t.get('source_case_id'), 20),
            safe_str(t.get('analyst_decision'), 40),
            safe_int(t.get('sar_filed')),
            safe_int(t.get('fraud_confirmed')),
            safe_float(t.get('fraud_loss')),
            safe_float(t.get('recovery_amount')),
            safe_str(t.get('disposition_reason')),
            safe_str(t.get('fp_reason'), 100),
            safe_int(t.get('investigation_time')),
            safe_str(t.get('notes')),
            safe_int(t.get('ml_score')),
            safe_str(t.get('ml_priority'), 20),
            safe_str(t.get('ml_typology'), 60),
        ))

    sql = """INSERT IGNORE INTO transactions VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s)"""
    n = executemany(sql, rows)
    print(f"[DB] Seeded {n} transactions", flush=True)
    return n


def seed_alerts(alerts):
    """Bulk-insert alerts."""
    rows = [(
        a['id'], a.get('entity',''), a.get('amount'), a.get('score'),
        a.get('priority'), a.get('typology'), a.get('channel'),
        a.get('timestamp'), a.get('status','open'), a.get('officer'),
        a.get('case_id'), a.get('txn_id'),
        a.get('sender_bank'), a.get('sender_country'),
        a.get('bene_name'), a.get('bene_bank'), a.get('bene_country'),
        a.get('bene_risk_score'), a.get('bene_blacklist'),
        a.get('kyc_level'), a.get('jurisdiction'), a.get('payment_rail'),
        a.get('cross_border'), a.get('velocity_3d'), a.get('behavioral_drift'),
        a.get('geo_location'),
        json.dumps(a.get('model_scores',{})),
        json.dumps(a.get('shap',[])),
        a.get('notes',''), a.get('source','dataset'),
    ) for a in alerts]
    sql = """INSERT IGNORE INTO alerts
        (id,entity,amount,score,priority,typology,channel,timestamp,
         status,officer,case_id,txn_id,sender_bank,sender_country,
         bene_name,bene_bank,bene_country,bene_risk_score,bene_blacklist,
         kyc_level,jurisdiction,payment_rail,cross_border,velocity_3d,
         behavioral_drift,geo_location,model_scores,shap_data,notes,source)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    n = executemany(sql, rows)
    print(f"[DB] Seeded {n} alerts", flush=True)
    return n


def seed_cases(cases):
    """Bulk-insert cases."""
    rows = [(
        c['id'], c.get('entity',''),
        json.dumps(c.get('alerts',[])),
        c.get('alert_count',0), c.get('priority'),
        c.get('status','open'), c.get('officer'),
        c.get('opened'), c.get('sar_due'),
        c.get('typology'), c.get('narrative',''),
        c.get('sar_status'),
    ) for c in cases]
    sql = """INSERT IGNORE INTO cases
        (id,entity,alert_ids,alert_count,priority,status,officer,
         opened,sar_due,typology,narrative,sar_status)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    n = executemany(sql, rows)
    print(f"[DB] Seeded {n} cases", flush=True)
    return n


def seed_default_users():
    """Insert default users if none exist."""
    count = execute("SELECT COUNT(*) as n FROM users", fetch="one")
    if (count or {}).get("n", 0) > 0:
        return
    import hashlib
    def h(p): return hashlib.sha256(p.encode()).hexdigest()
    users = [
        ('admin',    'Admin User',       'admin',      'Compliance', h('admin123')),
        ('ransford', 'Ransford Adjapong','supervisor', 'AML Ops',    h('pass123')),
        ('jmensah',  'J. Mensah',        'analyst',    'AML Ops',    h('pass123')),
        ('aowusu',   'A. Owusu',         'analyst',    'AML Ops',    h('pass123')),
        ('basante',  'B. Asante',        'analyst',    'AML Ops',    h('pass123')),
        ('kboateng', 'K. Boateng',       'analyst',    'AML Ops',    h('pass123')),
    ]
    sql = """INSERT IGNORE INTO users (username,display_name,role,department,password_hash)
             VALUES (%s,%s,%s,%s,%s)"""
    executemany(sql, users)
    print(f"[DB] Seeded {len(users)} default users", flush=True)


# ── Read helpers used by data_store ──────────────────────────────────────────
def load_alerts():
    rows = execute("SELECT * FROM alerts ORDER BY timestamp DESC", fetch="all") or []
    result = []
    for r in rows:
        r['model_scores'] = json.loads(r.get('model_scores') or '{}')
        r['shap']         = json.loads(r.get('shap_data')    or '[]')
        result.append(dict(r))
    return result


def load_cases():
    rows = execute("SELECT * FROM cases ORDER BY opened DESC", fetch="all") or []
    result = []
    for r in rows:
        r['alerts'] = json.loads(r.get('alert_ids') or '[]')
        result.append(dict(r))
    return result


def load_transactions(limit=1000):
    rows = execute(f"SELECT * FROM transactions LIMIT {limit}", fetch="all") or []
    return [dict(r) for r in rows]


def update_alert_status(alert_id, status):
    execute("UPDATE alerts SET status=%s WHERE id=%s", (status, alert_id))


def update_case_status(case_id, status, officer=None, escalated_by=None):
    if escalated_by:
        execute("UPDATE cases SET status=%s, escalated_by=%s, escalated_at=NOW() WHERE id=%s",
                (status, escalated_by, case_id))
    elif officer:
        execute("UPDATE cases SET status=%s, officer=%s WHERE id=%s",
                (status, officer, case_id))
    else:
        execute("UPDATE cases SET status=%s WHERE id=%s", (status, case_id))


def log_audit(user, action, target, detail='', ip=''):
    execute("INSERT INTO audit_log (user_name,action,target,detail,ip_address) VALUES (%s,%s,%s,%s,%s)",
            (user, action, target, detail, ip))


def add_notification(ntype, title, message, target_id='', severity='info'):
    execute("INSERT INTO notifications (type,title,message,target_id,severity,read_by) VALUES (%s,%s,%s,%s,%s,%s)",
            (ntype, title, message, target_id, severity, '[]'))


# ── SAR persistence ───────────────────────────────────────────────────────────
def persist_sar(sar: dict):
    """Insert or update a SAR record in the database."""
    try:
        execute("""
            INSERT INTO sar_records
                (id, case_id, alert_id, entity, filing_officer,
                 narrative, status, filed_at, fincen_ref, documents)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                status=VALUES(status),
                narrative=VALUES(narrative),
                filed_at=VALUES(filed_at),
                fincen_ref=VALUES(fincen_ref)
        """, (
            sar.get('id'), sar.get('case_id'), sar.get('alert_id'),
            sar.get('entity'), sar.get('officer') or sar.get('filing_officer'),
            sar.get('narrative',''), sar.get('status','filed'),
            sar.get('filed_date') or sar.get('filed_at'),
            sar.get('fincen_ref',''),
            json.dumps(sar.get('documents', [])),
        ))
    except Exception as e:
        print(f"[DB] SAR persist error: {e}", flush=True)


def load_sars():
    """Load all SAR records from database."""
    rows = execute("SELECT * FROM sar_records ORDER BY created_at DESC", fetch="all") or []
    result = []
    for r in rows:
        d = dict(r)
        for k, v in d.items():
            if hasattr(v, 'isoformat'): d[k] = str(v)
        for jk in ('documents',):
            if jk in d and isinstance(d[jk], str):
                try: d[jk] = json.loads(d[jk])
                except: pass
        result.append(d)
    return result


# ── Case persistence ──────────────────────────────────────────────────────────
def persist_case(case: dict):
    """Insert or update a case in the database."""
    try:
        execute("""
            INSERT INTO cases
                (id, entity, alert_ids, alert_count, priority, status,
                 officer, opened, sar_due, typology, narrative,
                 sar_status, escalated_by, escalated_at, manual_sar)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                status=VALUES(status),
                officer=VALUES(officer),
                sar_status=VALUES(sar_status),
                narrative=VALUES(narrative),
                escalated_by=VALUES(escalated_by),
                escalated_at=VALUES(escalated_at),
                alert_count=VALUES(alert_count),
                alert_ids=VALUES(alert_ids)
        """, (
            case.get('id'), case.get('entity',''),
            json.dumps(case.get('alerts',[])),
            case.get('alert_count', len(case.get('alerts',[]))),
            case.get('priority','medium'), case.get('status','open'),
            case.get('officer',''), case.get('opened'),
            case.get('sar_due'), case.get('typology',''),
            case.get('narrative',''), case.get('sar_status'),
            case.get('escalated_by'), case.get('escalated_at'),
            1 if case.get('manual_sar') else 0,
        ))
    except Exception as e:
        print(f"[DB] Case persist error: {e}", flush=True)


# ── Alert status persistence ──────────────────────────────────────────────────
def persist_alert(alert: dict):
    """Insert or update an alert in the database."""
    try:
        execute("""
            INSERT INTO alerts
                (id, entity, amount, score, priority, typology, channel,
                 timestamp, status, officer, case_id, txn_id,
                 sender_bank, sender_country, bene_name, bene_bank,
                 bene_country, bene_risk_score, bene_blacklist, kyc_level,
                 jurisdiction, payment_rail, cross_border, velocity_3d,
                 behavioral_drift, geo_location, model_scores, shap_data,
                 notes, source)
            VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                 %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                status=VALUES(status),
                officer=VALUES(officer),
                case_id=VALUES(case_id)
        """, (
            alert.get('id'), alert.get('entity',''),
            alert.get('amount'), alert.get('score'),
            alert.get('priority'), alert.get('typology',''),
            alert.get('channel',''), alert.get('timestamp'),
            alert.get('status','open'), alert.get('officer'),
            alert.get('case_id'), alert.get('txn_id'),
            alert.get('sender_bank',''), alert.get('sender_country',''),
            alert.get('bene_name',''), alert.get('bene_bank',''),
            alert.get('bene_country',''),
            alert.get('bene_risk_score', 0), alert.get('bene_blacklist', 0),
            alert.get('kyc_level',''), alert.get('jurisdiction',''),
            alert.get('payment_rail',''), alert.get('cross_border', 0),
            alert.get('velocity_3d', 0), alert.get('behavioral_drift', 0),
            alert.get('geo_location',''),
            json.dumps(alert.get('model_scores',{})),
            json.dumps(alert.get('shap',[])),
            alert.get('notes',''), alert.get('source','dataset'),
        ))
    except Exception as e:
        print(f"[DB] Alert persist error: {e}", flush=True)


def is_db_available():
    """Check if database is configured and reachable."""
    import os as _os
    if not (_os.environ.get('DATABASE_URL') or _os.environ.get('MYSQLHOST')):
        return False
    try:
        get_conn()
        return True
    except Exception:
        return False
