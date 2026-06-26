"""
Insurance Data Platform — Python Analytics Layer
=================================================
Phase 7: Data profiling, visualization, and PDF report
Charts:
    1. Monthly Premium Trend
    2. Premium by County (Bar)
    3. Policy Type Distribution (Pie)
    4. Claims by Status (Bar)
    5. Loss Ratio by Policy Type
    6. Agent Performance Leaderboard
    7. Payment Method Breakdown
    8. Late Payment Rate by Frequency
    9. Customer Age Band Distribution
   10. Monthly Collections vs Premium
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import matplotlib
matplotlib.use("Agg")  # non-interactive backend for saving files
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.ticker import FuncFormatter
import matplotlib.patches as mpatches
import numpy as np
from datetime import datetime
from sqlalchemy import text
from config.db import engine
from config.gcp import upload_to_gcs, GCS_BUCKET

# Create or replace the summary_stats view now that all fact tables exist
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS summary_stats CASCADE"))
    conn.execute(text("DROP VIEW IF EXISTS summary_stats CASCADE"))
    conn.execute(text("""
        CREATE OR REPLACE VIEW summary_stats AS
        SELECT 'Total Customers'       AS metric, COUNT(*)::text AS value FROM customers
        UNION ALL
        SELECT 'Total Policies',        COUNT(*)::text            FROM policies
        UNION ALL
        SELECT 'Total Claims',          COUNT(*)::text            FROM claims
        UNION ALL
        SELECT 'Gross Premium (KES)',   ROUND(SUM(premium_amount), 2)::text
            FROM fact_sales
        UNION ALL
        SELECT 'Total Collected (KES)', ROUND(SUM(payment_amount), 2)::text
            FROM fact_payments
            WHERE payment_status = 'Completed'
    """))
print("summary_stats view ready")

# -----------------------------
# STYLE CONFIG
# -----------------------------
plt.rcParams.update({
    "figure.facecolor":  "#0f1117",
    "axes.facecolor":    "#1a1d27",
    "axes.edgecolor":    "#2e3250",
    "axes.labelcolor":   "#c9d1d9",
    "axes.titlecolor":   "#ffffff",
    "xtick.color":       "#8b949e",
    "ytick.color":       "#8b949e",
    "text.color":        "#c9d1d9",
    "grid.color":        "#2e3250",
    "grid.linestyle":    "--",
    "grid.alpha":        0.5,
    "font.family":       "DejaVu Sans",
    "font.size":         10,
    "axes.titlesize":    13,
    "axes.labelsize":    10,
})

COLORS = {
    "primary":   "#4f8ef7",
    "success":   "#3fb950",
    "warning":   "#f0a040",
    "danger":    "#f85149",
    "purple":    "#bc8cff",
    "cyan":      "#39d0d8",
    "pink":      "#ff7eb6",
    "gold":      "#ffd700",
}

PALETTE = [
    COLORS["primary"], COLORS["success"], COLORS["warning"],
    COLORS["danger"],  COLORS["purple"],  COLORS["cyan"],
]

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "reports")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def kes(x, pos):
    """Format axis ticks as KES thousands/millions."""
    if x >= 1_000_000:
        return f"KES {x/1_000_000:.1f}M"
    elif x >= 1_000:
        return f"KES {x/1_000:.0f}K"
    return f"KES {x:.0f}"

print("Insurance Analytics Platform")
print("=" * 55)

# ============================================================
# EXTRACT ALL DATA
# ============================================================
print("Loading data from PostgreSQL...")

monthly_premium = pd.read_sql("""
    SELECT
        d.year, d.month, d.month_name,
        ROUND(SUM(fs.premium_amount), 2)   AS premium,
        ROUND(SUM(fs.commission_rate * fs.premium_amount),2) AS commission,
        COUNT(fs.sale_id)                  AS policies
    FROM fact_sales fs
    JOIN dim_date d ON fs.date_key = d.date_key
    WHERE d.year BETWEEN EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '3 years') AND EXTRACT(YEAR FROM CURRENT_DATE)
    GROUP BY d.year, d.month, d.month_name
    ORDER BY d.year, d.month
""", engine)

county_premium = pd.read_sql("""
    SELECT
        dc.county_name,
        ROUND(SUM(fs.premium_amount), 2)  AS total_premium,
        COUNT(fs.sale_id)                 AS policies
    FROM fact_sales fs
    JOIN dim_county dc ON fs.county_id = dc.county_id
    GROUP BY dc.county_name
    ORDER BY total_premium DESC
""", engine)

policy_type_dist = pd.read_sql("""
    SELECT
        policy_type,
        COUNT(*)                          AS count,
        ROUND(SUM(premium_amount), 2)     AS total_premium
    FROM fact_sales
    GROUP BY policy_type
    ORDER BY count DESC
""", engine)

claims_status = pd.read_sql("""
    SELECT
        cs.status_name,
        COUNT(fc.claim_id)                AS claims,
        ROUND(SUM(fc.claim_amount), 2)    AS total_claimed,
        ROUND(SUM(fc.approved_amount), 2) AS total_approved
    FROM fact_claims fc
    JOIN dim_claim_status cs ON fc.status_id = cs.status_id
    GROUP BY cs.status_name
    ORDER BY claims DESC
""", engine)

loss_ratio = pd.read_sql("""
    SELECT
        pt.policy_type,
        pt.risk_level,
        ROUND(SUM(fs.premium_amount), 2)    AS total_premium,
        ROUND(SUM(fc.approved_amount), 2)   AS total_approved,
        ROUND(SUM(fc.approved_amount) /
            NULLIF(SUM(fs.premium_amount),0) * 100, 2) AS loss_ratio_pct
    FROM dim_policy_type pt
    LEFT JOIN fact_sales  fs ON fs.policy_type_id = pt.policy_type_id
    LEFT JOIN fact_claims fc ON fc.policy_type_id = pt.policy_type_id
    GROUP BY pt.policy_type, pt.risk_level
    ORDER BY loss_ratio_pct DESC
""", engine)

agent_perf = pd.read_sql("""
    SELECT
        da.agent_name,
        da.channel,
        COUNT(fs.sale_id)                   AS policies,
        ROUND(SUM(fs.premium_amount), 2)    AS total_premium,
        ROUND(SUM(fs.commission_rate * fs.premium_amount), 2) AS commission
    FROM fact_sales fs
    JOIN dim_agent da ON fs.agent_id::text = da.agent_id::text
    GROUP BY da.agent_id, da.agent_name, da.channel
    ORDER BY total_premium DESC
    LIMIT 10
""", engine)

payment_method = pd.read_sql("""
    SELECT
        payment_method,
        COUNT(*)                           AS payments,
        ROUND(SUM(payment_amount), 2)      AS total_collected
    FROM fact_payments
    WHERE payment_status = 'Completed'
    GROUP BY payment_method
    ORDER BY total_collected DESC
""", engine)

late_by_freq = pd.read_sql("""
    SELECT
        payment_frequency,
        COUNT(*)                                        AS total,
        COUNT(*) FILTER (WHERE is_late = TRUE)          AS late,
        ROUND(COUNT(*) FILTER (WHERE is_late = TRUE)
            * 100.0 / COUNT(*), 1)                      AS late_pct
    FROM fact_payments
    GROUP BY payment_frequency
    ORDER BY late_pct DESC
""", engine)

age_band = pd.read_sql("""
    SELECT
        age_band,
        COUNT(*)                            AS customers,
        ROUND(AVG(fs.premium_amount), 2)    AS avg_premium
    FROM dim_customer dc
    LEFT JOIN fact_sales fs ON dc.customer_id::text = fs.customer_id::text
    GROUP BY age_band
    ORDER BY age_band
""", engine)

collections_vs_premium = pd.read_sql("""
    SELECT
        d.year,
        d.month,
        d.month_name,
        ROUND(SUM(fs.premium_amount), 2)    AS gross_premium,
        ROUND(SUM(fp.payment_amount)
            FILTER (WHERE fp.payment_status = 'Completed'), 2) AS collected
    FROM dim_date d
    LEFT JOIN fact_sales    fs ON fs.date_key = d.date_key
    LEFT JOIN fact_payments fp ON fp.date_key = d.date_key
    WHERE d.year BETWEEN EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '3 years') AND EXTRACT(YEAR FROM CURRENT_DATE)
    GROUP BY d.year, d.month, d.month_name
    ORDER BY d.year, d.month
""", engine)

summary = pd.read_sql("SELECT * FROM summary_stats", engine)

print("Data loaded successfully")
print(f"   Monthly records  : {len(monthly_premium)}")
print(f"   Counties         : {len(county_premium)}")
print(f"   Claim statuses   : {len(claims_status)}")
print(f"   Agents           : {len(agent_perf)}")

# ============================================================
# CHART 1: MONTHLY PREMIUM TREND (Line)
# ============================================================
print("\nBuilding Chart 1: Monthly Premium Trend...")

fig, ax = plt.subplots(figsize=(14, 5))
fig.patch.set_facecolor("#0f1117")

monthly_premium["period"] = (
    monthly_premium["month_name"].str[:3] + " " +
    monthly_premium["year"].astype(str)
)

ax.fill_between(
    range(len(monthly_premium)),
    monthly_premium["premium"],
    alpha=0.2, color=COLORS["primary"]
)
ax.plot(
    range(len(monthly_premium)),
    monthly_premium["premium"],
    color=COLORS["primary"], linewidth=2.5, marker="o", markersize=4
)
ax.plot(
    range(len(monthly_premium)),
    monthly_premium["commission"],
    color=COLORS["warning"], linewidth=1.5,
    linestyle="--", label="Commission"
)

ax.set_xticks(range(len(monthly_premium)))
ax.set_xticklabels(monthly_premium["period"], rotation=45, ha="right", fontsize=8)
ax.yaxis.set_major_formatter(FuncFormatter(kes))
ax.set_title("Monthly Gross Premium Trend", fontweight="bold", pad=15)
ax.set_ylabel("Premium Amount")
ax.legend(facecolor="#1a1d27", edgecolor="#2e3250")
ax.grid(True, axis="y")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/01_monthly_premium_trend.png", dpi=150, bbox_inches="tight")
plt.close()
print("   Saved: 01_monthly_premium_trend.png")


# ============================================================
# CHART 2: PREMIUM BY COUNTY (Horizontal Bar)
# ============================================================
print("Building Chart 2: Premium by County...")

fig, ax = plt.subplots(figsize=(10, 5))
fig.patch.set_facecolor("#0f1117")

bars = ax.barh(
    county_premium["county_name"],
    county_premium["total_premium"],
    color=PALETTE[:len(county_premium)],
    edgecolor="none", height=0.6
)

for bar, val in zip(bars, county_premium["total_premium"]):
    ax.text(
        bar.get_width() + county_premium["total_premium"].max() * 0.01,
        bar.get_y() + bar.get_height() / 2,
        f"KES {val/1_000_000:.2f}M",
        va="center", fontsize=9, color="#c9d1d9"
    )

ax.xaxis.set_major_formatter(FuncFormatter(kes))
ax.set_title("Total Premium by County", fontweight="bold", pad=15)
ax.set_xlabel("Total Premium (KES)")
ax.grid(True, axis="x")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/02_premium_by_county.png", dpi=150, bbox_inches="tight")
plt.close()
print("   Saved: 02_premium_by_county.png")


# ============================================================
# CHART 3: POLICY TYPE DISTRIBUTION (Donut)
# ============================================================
print("Building Chart 3: Policy Type Distribution...")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
fig.patch.set_facecolor("#0f1117")

# Donut — policy count
wedges, texts, autotexts = ax1.pie(
    policy_type_dist["count"],
    labels=policy_type_dist["policy_type"],
    colors=PALETTE[:len(policy_type_dist)],
    autopct="%1.1f%%",
    startangle=90,
    wedgeprops={"width": 0.6, "edgecolor": "#0f1117", "linewidth": 2},
    pctdistance=0.75
)
for at in autotexts:
    at.set_color("white")
    at.set_fontsize(10)
ax1.set_title("Policy Count by Type", fontweight="bold")

# Bar — premium by type
bars = ax2.bar(
    policy_type_dist["policy_type"],
    policy_type_dist["total_premium"],
    color=PALETTE[:len(policy_type_dist)],
    edgecolor="none", width=0.5
)
ax2.yaxis.set_major_formatter(FuncFormatter(kes))
ax2.set_title("Premium by Policy Type", fontweight="bold")
ax2.set_ylabel("Total Premium")
ax2.grid(True, axis="y")

for bar, val in zip(bars, policy_type_dist["total_premium"]):
    ax2.text(
        bar.get_x() + bar.get_width()/2,
        bar.get_height() + policy_type_dist["total_premium"].max() * 0.01,
        f"KES {val/1_000_000:.1f}M",
        ha="center", fontsize=9, color="#c9d1d9"
    )

plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/03_policy_type_distribution.png", dpi=150, bbox_inches="tight")
plt.close()
print("   Saved: 03_policy_type_distribution.png")


# ============================================================
# CHART 4: CLAIMS BY STATUS (Grouped Bar)
# ============================================================
print("Building Chart 4: Claims by Status...")

fig, ax = plt.subplots(figsize=(11, 5))
fig.patch.set_facecolor("#0f1117")

x      = np.arange(len(claims_status))
width  = 0.35

ax.bar(x - width/2, claims_status["total_claimed"],
       width, label="Total Claimed",  color=COLORS["warning"], edgecolor="none")
ax.bar(x + width/2, claims_status["total_approved"],
       width, label="Total Approved", color=COLORS["success"], edgecolor="none")

ax.set_xticks(x)
ax.set_xticklabels(claims_status["status_name"], fontsize=10)
ax.yaxis.set_major_formatter(FuncFormatter(kes))
ax.set_title("Claims: Claimed vs Approved by Status", fontweight="bold", pad=15)
ax.set_ylabel("Amount (KES)")
ax.legend(facecolor="#1a1d27", edgecolor="#2e3250")
ax.grid(True, axis="y")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/04_claims_by_status.png", dpi=150, bbox_inches="tight")
plt.close()
print("   Saved: 04_claims_by_status.png")


# ============================================================
# CHART 5: LOSS RATIO BY POLICY TYPE (Bar with threshold line)
# ============================================================
print("Building Chart 5: Loss Ratio...")

fig, ax = plt.subplots(figsize=(9, 5))
fig.patch.set_facecolor("#0f1117")

colors_lr = [
    COLORS["danger"] if v > 100 else
    COLORS["warning"] if v > 70 else
    COLORS["success"]
    for v in loss_ratio["loss_ratio_pct"]
]

bars = ax.bar(
    loss_ratio["policy_type"],
    loss_ratio["loss_ratio_pct"],
    color=colors_lr, edgecolor="none", width=0.5
)

# Threshold lines
ax.axhline(y=100, color=COLORS["danger"],  linestyle="--", linewidth=1.5, label="Break-even (100%)")
ax.axhline(y=70,  color=COLORS["warning"], linestyle="--", linewidth=1.0, label="Warning (70%)")

for bar, val in zip(bars, loss_ratio["loss_ratio_pct"]):
    ax.text(
        bar.get_x() + bar.get_width()/2,
        bar.get_height() + 1,
        f"{val}%",
        ha="center", fontsize=11, fontweight="bold", color="white"
    )

ax.set_title("Loss Ratio by Policy Type", fontweight="bold", pad=15)
ax.set_ylabel("Loss Ratio (%)")
ax.legend(facecolor="#1a1d27", edgecolor="#2e3250")
ax.grid(True, axis="y")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/05_loss_ratio.png", dpi=150, bbox_inches="tight")
plt.close()
print("   Saved: 05_loss_ratio.png")


# ============================================================
# CHART 6: AGENT PERFORMANCE LEADERBOARD
# ============================================================
print("Building Chart 6: Agent Performance...")

fig, ax = plt.subplots(figsize=(12, 6))
fig.patch.set_facecolor("#0f1117")

channel_colors = {
    "Direct": COLORS["primary"],
    "Broker": COLORS["success"],
    "Online": COLORS["purple"],
    "Bancassurance": COLORS["cyan"]
}
bar_colors = [channel_colors.get(c, COLORS["primary"]) for c in agent_perf["channel"]]

bars = ax.barh(
    agent_perf["agent_name"],
    agent_perf["total_premium"],
    color=bar_colors, edgecolor="none", height=0.6
)

for bar, val, comm in zip(bars, agent_perf["total_premium"], agent_perf["commission"]):
    ax.text(
        bar.get_width() + agent_perf["total_premium"].max() * 0.01,
        bar.get_y() + bar.get_height()/2,
        f"KES {val/1_000:.0f}K  |  Comm: KES {comm/1_000:.0f}K",
        va="center", fontsize=8, color="#c9d1d9"
    )

legend_patches = [
    mpatches.Patch(color=v, label=k)
    for k, v in channel_colors.items()
]
ax.legend(handles=legend_patches, facecolor="#1a1d27",
          edgecolor="#2e3250", loc="lower right")
ax.xaxis.set_major_formatter(FuncFormatter(kes))
ax.set_title("Top 10 Agents by Premium (with Channel)", fontweight="bold", pad=15)
ax.set_xlabel("Total Premium")
ax.grid(True, axis="x")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/06_agent_performance.png", dpi=150, bbox_inches="tight")
plt.close()
print("   Saved: 06_agent_performance.png")


# ============================================================
# CHART 7: PAYMENT METHOD BREAKDOWN (Donut)
# ============================================================
print("Building Chart 7: Payment Methods...")

fig, ax = plt.subplots(figsize=(8, 6))
fig.patch.set_facecolor("#0f1117")

wedges, texts, autotexts = ax.pie(
    payment_method["total_collected"],
    labels=payment_method["payment_method"],
    colors=PALETTE[:len(payment_method)],
    autopct="%1.1f%%",
    startangle=90,
    wedgeprops={"width": 0.55, "edgecolor": "#0f1117", "linewidth": 2},
    pctdistance=0.75
)
for at in autotexts:
    at.set_color("white")
    at.set_fontsize(11)

# Center text
total = payment_method["total_collected"].sum()
ax.text(0, 0, f"KES\n{total/1_000_000:.1f}M",
        ha="center", va="center",
        fontsize=13, fontweight="bold", color="white")

ax.set_title("Payment Collections by Method", fontweight="bold", pad=15)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/07_payment_methods.png", dpi=150, bbox_inches="tight")
plt.close()
print("   Saved: 07_payment_methods.png")


# ============================================================
# CHART 8: LATE PAYMENT RATE BY FREQUENCY
# ============================================================
print("Building Chart 8: Late Payment Rates...")

fig, ax = plt.subplots(figsize=(9, 5))
fig.patch.set_facecolor("#0f1117")

x     = np.arange(len(late_by_freq))
width = 0.35

b1 = ax.bar(x - width/2, late_by_freq["total"], width,
            label="Total",  color=COLORS["primary"],  edgecolor="none")
b2 = ax.bar(x + width/2, late_by_freq["late"],  width,
            label="Late",   color=COLORS["danger"],   edgecolor="none")

ax2_twin = ax.twinx()
ax2_twin.plot(x, late_by_freq["late_pct"],
              color=COLORS["warning"], marker="D",
              linewidth=2, markersize=8, label="Late %")
ax2_twin.set_ylabel("Late Payment %", color=COLORS["warning"])
ax2_twin.tick_params(axis="y", labelcolor=COLORS["warning"])
ax2_twin.set_ylim(0, 100)

ax.set_xticks(x)
ax.set_xticklabels(late_by_freq["payment_frequency"])
ax.set_title("Late Payment Rate by Frequency", fontweight="bold", pad=15)
ax.set_ylabel("Payment Count")
ax.legend(facecolor="#1a1d27", edgecolor="#2e3250", loc="upper left")
ax.grid(True, axis="y")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/08_late_payments.png", dpi=150, bbox_inches="tight")
plt.close()
print("   Saved: 08_late_payments.png")


# ============================================================
# CHART 9: CUSTOMER AGE BAND DISTRIBUTION
# ============================================================
print("Building Chart 9: Customer Age Bands...")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
fig.patch.set_facecolor("#0f1117")

# Count bar
ax1.bar(age_band["age_band"], age_band["customers"],
        color=PALETTE[:len(age_band)], edgecolor="none", width=0.6)
ax1.set_title("Customers by Age Band", fontweight="bold")
ax1.set_ylabel("Customer Count")
ax1.grid(True, axis="y")

# Avg premium line
ax2.bar(age_band["age_band"], age_band["avg_premium"],
        color=PALETTE[:len(age_band)], edgecolor="none", width=0.6, alpha=0.8)
ax2.yaxis.set_major_formatter(FuncFormatter(kes))
ax2.set_title("Average Premium by Age Band", fontweight="bold")
ax2.set_ylabel("Avg Premium (KES)")
ax2.grid(True, axis="y")

plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/09_age_band_analysis.png", dpi=150, bbox_inches="tight")
plt.close()
print("   Saved: 09_age_band_analysis.png")


# ============================================================
# CHART 10: COLLECTIONS VS PREMIUM (Area Chart)
# ============================================================
print("Building Chart 10: Collections vs Premium...")

fig, ax = plt.subplots(figsize=(14, 5))
fig.patch.set_facecolor("#0f1117")

coll = collections_vs_premium.dropna()
coll["period"] = coll["month_name"].str[:3] + " " + coll["year"].astype(str)

ax.fill_between(range(len(coll)), coll["gross_premium"],
                alpha=0.3, color=COLORS["primary"], label="Gross Premium")
ax.fill_between(range(len(coll)), coll["collected"],
                alpha=0.4, color=COLORS["success"], label="Collected")
ax.plot(range(len(coll)), coll["gross_premium"],
        color=COLORS["primary"], linewidth=2)
ax.plot(range(len(coll)), coll["collected"],
        color=COLORS["success"], linewidth=2)

ax.set_xticks(range(len(coll)))
ax.set_xticklabels(coll["period"], rotation=45, ha="right", fontsize=8)
ax.yaxis.set_major_formatter(FuncFormatter(kes))
ax.set_title("Monthly: Gross Premium vs Collections", fontweight="bold", pad=15)
ax.set_ylabel("Amount (KES)")
ax.legend(facecolor="#1a1d27", edgecolor="#2e3250")
ax.grid(True, axis="y")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/10_collections_vs_premium.png", dpi=150, bbox_inches="tight")
plt.close()
print("   Saved: 10_collections_vs_premium.png")


# ============================================================
# DASHBOARD: All 10 charts in one image
# ============================================================
print("\nBuilding Master Dashboard...")

fig = plt.figure(figsize=(24, 30))
fig.patch.set_facecolor("#0f1117")
fig.suptitle(
    "INSURANCE DATA PLATFORM — ANALYTICS DASHBOARD",
    fontsize=20, fontweight="bold", color="white", y=0.98
)

charts = [
    "01_monthly_premium_trend.png",
    "02_premium_by_county.png",
    "03_policy_type_distribution.png",
    "04_claims_by_status.png",
    "05_loss_ratio.png",
    "06_agent_performance.png",
    "07_payment_methods.png",
    "08_late_payments.png",
    "09_age_band_analysis.png",
    "10_collections_vs_premium.png",
]

for i, chart in enumerate(charts):
    ax = fig.add_subplot(5, 2, i + 1)
    img = plt.imread(f"{OUTPUT_DIR}/{chart}")
    ax.imshow(img)
    ax.axis("off")

plt.tight_layout(rect=[0, 0, 1, 0.97])
plt.savefig(f"{OUTPUT_DIR}/00_master_dashboard.png",
            dpi=120, bbox_inches="tight")
plt.close()
print("   Saved: 00_master_dashboard.png")


# ============================================================
# PRINT SUMMARY STATS
# ============================================================
print("\n" + "=" * 55)
print("PLATFORM SUMMARY STATS")
print("=" * 55)
for _, row in summary.iterrows():
    print(f"  {row['metric']:<25} {row['value']}")
print("=" * 55)
print(f"\nAll charts saved to: {OUTPUT_DIR}/")
print("Analytics complete.")

# ── Upload reports to Cloud Storage ────────────────────────
print(f"\nUploading reports to gs://{GCS_BUCKET}/reports/ ...")
chart_files = [
    f for f in os.listdir(OUTPUT_DIR)
    if f.endswith(".png") and os.path.isfile(os.path.join(OUTPUT_DIR, f))
]
for fname in sorted(chart_files):
    local = os.path.join(OUTPUT_DIR, fname)
    gs_url = upload_to_gcs(local, f"reports/{fname}")
    print(f"  Uploaded: {gs_url}")
print(f"All reports uploaded to gs://{GCS_BUCKET}/reports/")