import pandas as pd

INP = "raw_data/phase4_canonical/pbp_events_canonical.csv"

df = pd.read_csv(INP, low_memory=False)
df["loc_x"] = pd.to_numeric(df.get("loc_x"), errors="coerce")
df["loc_y"] = pd.to_numeric(df.get("loc_y"), errors="coerce")

s = df.dropna(subset=["loc_x","loc_y"]).copy()

print("rows with coords:", len(s))
print("loc_x min/max:", s["loc_x"].min(), s["loc_x"].max())
print("loc_y min/max:", s["loc_y"].min(), s["loc_y"].max())

# show a few extreme examples
print("\nlowest x examples:\n", s.nsmallest(5, "loc_x")[["event_type","description","loc_x","loc_y","action_area"]])
print("\nhighest x examples:\n", s.nlargest(5, "loc_x")[["event_type","description","loc_x","loc_y","action_area"]])
print("\nlowest y examples:\n", s.nsmallest(5, "loc_y")[["event_type","description","loc_x","loc_y","action_area"]])
print("\nhighest y examples:\n", s.nlargest(5, "loc_y")[["event_type","description","loc_x","loc_y","action_area"]])
