# azure_batch
    Running a python application package in azure batch. Also schedule the application
    using azure batch job schedular


1.add batch account configuration to az_config.py

2.zip this package and upload to azure batch application packages in ur az batch account
and name the package name as azure_batch and version as 1

3.install azure-batch==6.0.0 in your machine

4.Run azure_batch_main.py


azure_batch_main.py:
	creates pools, jobs and a collection of tasks to the job and waits until all
	the tasks are completed . Then deletes the pool and the job.

az_config.py:
    Config for azure batch pool,job and tasks
    
node_startup_tasks.py:
    the startup tasks for the main pool. installs pip and installs the
    sample_requirements.txt in each compute nodes
    
sample_requirements.txt:
    the requirements that should be installed in the nodes for running the sample.py
    
job_schedular.py:
    creates a job schedular that runs the azure_batch_main.py every 15 days.

job_schedular_node_startup_tasks.py:
    the start up tasks for the job_schedular pool. installs pip and azure-batch
    to run the azure_batch_main.py
    
