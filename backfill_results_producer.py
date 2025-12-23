#!/usr/bin/env python3
from helpers.vl_results_publisher import publish_vl_results
from helpers.eid_scd_results_publisher import publish_eid_scd_results

# ---------- main run ----------

def main():
    publish_vl_results()
    publish_eid_scd_results()

if __name__ == "__main__":
    main()
