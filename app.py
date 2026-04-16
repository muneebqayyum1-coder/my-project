import sqlite3
from contextlib import closing
from datetime import datetime
import hashlib
import streamlit as st
import pandas as pd

DB_PATH = "dcms.db"


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def init_db():
    with closing(get_conn()) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('customer', 'admin', 'technician')),
                specialization TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS complaints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                complaint_code TEXT UNIQUE NOT NULL,
                customer_id INTEGER NOT NULL,
                installation_ref TEXT,
                category TEXT NOT NULL,
                priority TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                status TEXT NOT NULL,
                assigned_technician_id INTEGER,
                resolution_notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(customer_id) REFERENCES users(id),
                FOREIGN KEY(assigned_technician_id) REFERENCES users(id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS complaint_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                complaint_id INTEGER NOT NULL,
                updated_by INTEGER NOT NULL,
                old_status TEXT,
                new_status TEXT,
                note TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(complaint_id) REFERENCES complaints(id),
                FOREIGN KEY(updated_by) REFERENCES users(id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                complaint_id INTEGER UNIQUE NOT NULL,
                rating INTEGER NOT NULL,
                comments TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(complaint_id) REFERENCES complaints(id)
            )
            """
        )

        count = cur.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if count == 0:
            users = [
                ("Admin User", "admin@solarcms.com", hash_password("admin123"), "admin", None),
                ("Ali Technician", "tech1@solarcms.com", hash_password("tech123"), "technician", "Inverter Systems"),
                ("Sara Technician", "tech2@solarcms.com", hash_password("tech123"), "technician", "Panel Installation"),
                ("Muneeb Customer", "customer@solarcms.com", hash_password("cust123"), "customer", None),
            ]
            cur.executemany(
                "INSERT INTO users (full_name, email, password_hash, role, specialization) VALUES (?, ?, ?, ?, ?)",
                users,
            )

        conn.commit()


def authenticate(email: str, password: str):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT id, full_name, email, role FROM users WHERE email = ? AND password_hash = ?",
            (email, hash_password(password)),
        ).fetchone()
        return row


def register_customer(full_name: str, email: str, password: str):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (full_name, email, password_hash, role) VALUES (?, ?, ?, 'customer')",
            (full_name, email, hash_password(password)),
        )
        conn.commit()


def generate_complaint_code(complaint_id: int) -> str:
    return f"CMP-{datetime.now().strftime('%Y%m')}-{complaint_id:04d}"


def create_complaint(customer_id, installation_ref, category, priority, title, description):
    now = datetime.now().isoformat(timespec="seconds")
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO complaints (
                complaint_code, customer_id, installation_ref, category, priority, title,
                description, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'Submitted', ?, ?)
            """,
            ("TEMP", customer_id, installation_ref, category, priority, title, description, now, now),
        )
        complaint_id = cur.lastrowid
        code = generate_complaint_code(complaint_id)
        cur.execute("UPDATE complaints SET complaint_code = ? WHERE id = ?", (code, complaint_id))
        cur.execute(
            """
            INSERT INTO complaint_updates (complaint_id, updated_by, old_status, new_status, note, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (complaint_id, customer_id, None, "Submitted", "Complaint created by customer", now),
        )
        conn.commit()
    return code


def get_customer_complaints(customer_id):
    query = """
    SELECT complaint_code, category, priority, title, status, created_at, updated_at
    FROM complaints
    WHERE customer_id = ?
    ORDER BY id DESC
    """
    return pd.read_sql_query(query, get_conn(), params=(customer_id,))


def get_all_complaints():
    query = """
    SELECT c.id, c.complaint_code, u.full_name AS customer, c.installation_ref, c.category,
           c.priority, c.title, c.status, t.full_name AS technician, c.created_at, c.updated_at
    FROM complaints c
    JOIN users u ON c.customer_id = u.id
    LEFT JOIN users t ON c.assigned_technician_id = t.id
    ORDER BY c.id DESC
    """
    return pd.read_sql_query(query, get_conn())


def get_technicians():
    query = "SELECT id, full_name, specialization FROM users WHERE role = 'technician' ORDER BY full_name"
    return pd.read_sql_query(query, get_conn())


def assign_complaint(complaint_id: int, technician_id: int, admin_id: int, new_status: str = "Assigned"):
    now = datetime.now().isoformat(timespec="seconds")
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        old = cur.execute("SELECT status FROM complaints WHERE id = ?", (complaint_id,)).fetchone()[0]
        cur.execute(
            "UPDATE complaints SET assigned_technician_id = ?, status = ?, updated_at = ? WHERE id = ?",
            (technician_id, new_status, now, complaint_id),
        )
        cur.execute(
            """
            INSERT INTO complaint_updates (complaint_id, updated_by, old_status, new_status, note, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (complaint_id, admin_id, old, new_status, "Complaint assigned to technician", now),
        )
        conn.commit()


def get_technician_complaints(technician_id):
    query = """
    SELECT c.id, c.complaint_code, u.full_name AS customer, c.category, c.priority,
           c.title, c.description, c.status, c.created_at, c.updated_at
    FROM complaints c
    JOIN users u ON c.customer_id = u.id
    WHERE c.assigned_technician_id = ?
    ORDER BY c.id DESC
    """
    return pd.read_sql_query(query, get_conn(), params=(technician_id,))


def update_complaint_status(complaint_id: int, user_id: int, new_status: str, note: str, resolution_notes=None):
    now = datetime.now().isoformat(timespec="seconds")
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        old = cur.execute("SELECT status FROM complaints WHERE id = ?", (complaint_id,)).fetchone()[0]
        cur.execute(
            "UPDATE complaints SET status = ?, resolution_notes = COALESCE(?, resolution_notes), updated_at = ? WHERE id = ?",
            (new_status, resolution_notes, now, complaint_id),
        )
        cur.execute(
            """
            INSERT INTO complaint_updates (complaint_id, updated_by, old_status, new_status, note, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (complaint_id, user_id, old, new_status, note, now),
        )
        conn.commit()


def get_complaint_history(complaint_code: str):
    query = """
    SELECT c.id, c.complaint_code, c.title, c.description, c.status, c.priority,
           c.category, c.installation_ref, c.resolution_notes
    FROM complaints c
    WHERE c.complaint_code = ?
    """
    complaint = pd.read_sql_query(query, get_conn(), params=(complaint_code,))

    history_q = """
    SELECT cu.old_status, cu.new_status, cu.note, cu.created_at, u.full_name AS updated_by
    FROM complaint_updates cu
    JOIN users u ON cu.updated_by = u.id
    JOIN complaints c ON cu.complaint_id = c.id
    WHERE c.complaint_code = ?
    ORDER BY cu.id ASC
    """
    history = pd.read_sql_query(history_q, get_conn(), params=(complaint_code,))
    return complaint, history


def save_feedback(complaint_code: str, rating: int, comments: str):
    now = datetime.now().isoformat(timespec="seconds")
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        row = cur.execute("SELECT id FROM complaints WHERE complaint_code = ?", (complaint_code,)).fetchone()
        if not row:
            raise ValueError("Complaint not found")
        complaint_id = row[0]
        cur.execute(
            "INSERT OR REPLACE INTO feedback (complaint_id, rating, comments, created_at) VALUES (?, ?, ?, ?)",
            (complaint_id, rating, comments, now),
        )
        conn.commit()


def get_dashboard_metrics():
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        total = cur.execute("SELECT COUNT(*) FROM complaints").fetchone()[0]
        submitted = cur.execute("SELECT COUNT(*) FROM complaints WHERE status = 'Submitted'").fetchone()[0]
        in_progress = cur.execute("SELECT COUNT(*) FROM complaints WHERE status IN ('Assigned', 'In Progress')").fetchone()[0]
        resolved = cur.execute("SELECT COUNT(*) FROM complaints WHERE status IN ('Resolved', 'Closed')").fetchone()[0]
    return total, submitted, in_progress, resolved


def show_login():
    st.subheader("Login")
    email = st.text_input("Email")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        user = authenticate(email, password)
        if user:
            st.session_state.user = {
                "id": user[0],
                "full_name": user[1],
                "email": user[2],
                "role": user[3],
            }
            st.rerun()
        st.error("Invalid email or password")

    st.markdown("---")
    st.subheader("Register as Customer")
    full_name = st.text_input("Full name")
    reg_email = st.text_input("Customer email")
    reg_password = st.text_input("Create password", type="password")
    if st.button("Register"):
        try:
            register_customer(full_name, reg_email, reg_password)
            st.success("Customer account created. You can now log in.")
        except sqlite3.IntegrityError:
            st.error("Email already exists.")

    st.info(
        "Demo accounts:\n\n"
        "Admin: admin@solarcms.com / admin123\n\n"
        "Technician: tech1@solarcms.com / tech123\n\n"
        "Customer: customer@solarcms.com / cust123"
    )


def customer_view(user):
    st.title("Customer Portal")
    tab1, tab2, tab3 = st.tabs(["Submit Complaint", "My Complaints", "Track & Feedback"])

    with tab1:
        st.subheader("Submit a New Complaint")
        with st.form("complaint_form"):
            installation_ref = st.text_input("Installation Reference")
            category = st.selectbox(
                "Complaint Category",
                [
                    "Installation Fault",
                    "Inverter Problem",
                    "Low Energy Performance",
                    "Maintenance Delay",
                    "Warranty Claim",
                    "Billing / Service Issue",
                ],
            )
            priority = st.selectbox("Priority", ["Low", "Medium", "High"])
            title = st.text_input("Complaint Title")
            description = st.text_area("Describe the issue")
            submitted = st.form_submit_button("Submit Complaint")
            if submitted:
                code = create_complaint(user["id"], installation_ref, category, priority, title, description)
                st.success(f"Complaint submitted successfully. Your complaint code is {code}")

    with tab2:
        st.subheader("My Complaints")
        df = get_customer_complaints(user["id"])
        st.dataframe(df, use_container_width=True)

    with tab3:
        st.subheader("Track Complaint")
        complaint_code = st.text_input("Enter complaint code")
        if st.button("Load Complaint History"):
            complaint, history = get_complaint_history(complaint_code)
            if complaint.empty:
                st.warning("No complaint found.")
            else:
                st.write("### Complaint Details")
                st.dataframe(complaint, use_container_width=True)
                st.write("### Status History")
                st.dataframe(history, use_container_width=True)

        st.markdown("---")
        st.subheader("Submit Feedback")
        feedback_code = st.text_input("Complaint code for feedback")
        rating = st.slider("Rating", 1, 5, 4)
        comments = st.text_area("Comments")
        if st.button("Save Feedback"):
            try:
                save_feedback(feedback_code, rating, comments)
                st.success("Feedback saved successfully")
            except ValueError:
                st.error("Invalid complaint code")


def admin_view(user):
    st.title("Admin Dashboard")
    total, submitted, in_progress, resolved = get_dashboard_metrics()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total", total)
    c2.metric("Submitted", submitted)
    c3.metric("In Progress", in_progress)
    c4.metric("Resolved/Closed", resolved)

    st.subheader("All Complaints")
    complaints = get_all_complaints()
    st.dataframe(complaints, use_container_width=True)

    st.markdown("---")
    st.subheader("Assign Complaint")
    if not complaints.empty:
        complaint_id = st.selectbox("Complaint ID", complaints["id"].tolist())
        techs = get_technicians()
        tech_label_map = {
            f"{row['full_name']} ({row['specialization']})": int(row["id"]) for _, row in techs.iterrows()
        }
        tech_choice = st.selectbox("Assign to Technician", list(tech_label_map.keys()))
        if st.button("Assign Selected Complaint"):
            assign_complaint(complaint_id, tech_label_map[tech_choice], user["id"])
            st.success("Complaint assigned")
            st.rerun()

    st.markdown("---")
    st.subheader("Update Complaint Status")
    if not complaints.empty:
        complaint_id2 = st.selectbox("Complaint ID to update", complaints["id"].tolist(), key="admin_update")
        new_status = st.selectbox("New status", ["Submitted", "Assigned", "In Progress", "Resolved", "Closed"])
        note = st.text_area("Admin note")
        resolution_notes = st.text_area("Resolution notes (optional)")
        if st.button("Save Status Update"):
            update_complaint_status(complaint_id2, user["id"], new_status, note, resolution_notes)
            st.success("Complaint updated")
            st.rerun()


def technician_view(user):
    st.title("Technician Workspace")
    complaints = get_technician_complaints(user["id"])
    st.subheader("Assigned Complaints")
    st.dataframe(complaints, use_container_width=True)

    st.markdown("---")
    st.subheader("Update Assigned Complaint")
    if not complaints.empty:
        complaint_id = st.selectbox("Complaint ID", complaints["id"].tolist())
        new_status = st.selectbox("Status", ["In Progress", "Resolved", "Closed"])
        note = st.text_area("Technician update")
        resolution_notes = st.text_area("Resolution details")
        if st.button("Submit Update"):
            update_complaint_status(complaint_id, user["id"], new_status, note, resolution_notes)
            st.success("Update submitted")
            st.rerun()
    else:
        st.info("No complaints assigned yet.")


def main():
    st.set_page_config(page_title="Solar Complaint Management System", layout="wide")
    init_db()

    st.sidebar.title("Digital Complaint Management System")
    st.sidebar.caption("For UK Solar Installation Companies")

    if "user" not in st.session_state:
        st.session_state.user = None

    if st.session_state.user is None:
        show_login()
        return

    user = st.session_state.user
    st.sidebar.success(f"Logged in as {user['full_name']}")
    st.sidebar.write(f"Role: {user['role'].title()}")
    if st.sidebar.button("Logout"):
        st.session_state.user = None
        st.rerun()

    if user["role"] == "customer":
        customer_view(user)
    elif user["role"] == "admin":
        admin_view(user)
    elif user["role"] == "technician":
        technician_view(user)


if __name__ == "__main__":
    main()