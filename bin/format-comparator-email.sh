#!/bin/bash

# Create a temporary file for the complete email content
cat << 'EOF' > email_content.txt
Hi Team,

Please find the JDBC driver comparison results below. These differences were detected during our automated comparison test run.

*** Comparator Output Start ***

EOF

# Append the JDBC comparison report
cat jdbc-comparison-report.txt >> email_content.txt

# Append the footer message
cat << 'EOF' >> email_content.txt
*** Comparator Output End ***

Important Notes:
- If you believe any of these differences are acceptable and should be excluded from future reports, please contact jayant.singh@databricks.com with your reasoning and approval.
- To suggest additional metadata methods or SQL queries for comparison, please reach out to jayant.singh@databricks.com.

Thanks!
JDBC Comparator Runner
EOF

# Move the formatted content to the report file
mv email_content.txt jdbc-comparison-report.txt