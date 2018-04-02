# py-reports
This project is a boilerplate for the people trying to solve automated mysql reporting using python scripts 

Pre-requisites to run this project
Python 3.x installed on the system
pandas and mysql library installed (can be installed easily using pip)

SMTP mode setup on sender's mailing server

At the end of it, you just need to write a script to schedule your reports.py at a specific time.
Taking into consideration that the user is using this linux environment, I am writing a cron expression which schedules my script at 7 am IST everyday

00 7 * * * sudo sh <path_to_parent_dir_of>/run_reports.sh > <path_to_store_cron_execution_log>/cron.log 2> 1