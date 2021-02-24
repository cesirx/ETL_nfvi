# NFVi data ETL process
Python script that extracts, transforms and load data obtained from different layers in a NFVi architecture:
- Compute Manager (vCenter SOAP API)
- Hypervisors (ESXi)
- Out of band management interfaces (iDRAC, iLO)
- Network switches (forwarding db, lldp, and other L2 info)

Returns:
- CSV output file
- JSON output file
- Javascript-formatted HTML tables with data structured in a sortable tables
  
Tested in:
    vCenter 6.5u3 and 6.7
