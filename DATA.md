# bbmp-citizen-grievances

The source data is fetched from the [Bengaluru Smart City Limited Website](https://smartoneblr.com/NicApplicationStatus.htm)

## Data dictionary

| Variable | Type | Description |
|----------|------|-------------|
| Complaint ID | string | Unique ID for each grievance |
| Category | string | Category of the grievance (e.g., Electrical, Solid Waste, Road Maintenance) |
| Sub Category | string | Sub-category of the grievance (e.g., Garbage Dump, Potholes, Street Light Not Working) |
| Grievance Date | datetime | Date and time when the grievance was filed |
| Ward Name | string | BBMP ward where the grievance was reported |
| Grievance Status | string | Status of the grievance (e.g., Closed, Rejected) |
| Staff Remarks | string | Comments or notes added by BBMP staff |
| Staff Name | string | Name and designation of the staff handling the grievance |