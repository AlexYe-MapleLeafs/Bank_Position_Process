# Bank_Position_Process

**Automated Bank Loan and Investment Status Pipeline — Google Cloud Platform**

This repository contains a simplified, demonstration version of a real-world GCP project.
It illustrates the structure, configurations, and code required to automate a bank loan and investment status process using **Google Cloud Platform** services.

---

## Repository Structure

| File/Folder            | Purpose                                                                                                                                                                                                              |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`scripts/`**         | Python scripts containing SQL queries for extracting data and loading it into **BigQuery** target tables. <br>_Note: The production environment contains 12+ scripts. This demo includes only a subset for clarity._ |
| **`accp.yaml`**        | Pipeline configuration file defining build instructions for the container image, including image name, build context, and other ACCP pipeline parameters. <br>_Referenced during pipeline initialization._           |
| **`Dockerfile`**       | Image build specification defining the base image, system dependencies, and Python packages required to execute the scripts. <br>_Referenced in `accp.yaml`._                                                        |
| **`requirements.txt`** | List of Python dependencies to be installed in the image environment. <br>_Referenced in the `Dockerfile`._                                                                                                          |
| **`positions_dag.py`** | **Apache Airflow DAG** for orchestrating and scheduling the end-to-end process. References the container image built from the above files and defines execution logic and scheduling parameters.                     |

---

## Workflow Overview

1. **Code and Configuration** — Python scripts, configuration files, and dependencies are stored in this repository.
2. **Image Build** — The ACCP pipeline uses `accp.yaml`, `Dockerfile` and `requirement.txt` to build a container image with all required dependencies.
3. **Data Processing** — The container runs Python scripts to fetch, transform, and load data into BigQuery.
4. **Orchestration** — Airflow triggers the container execution according to the schedule defined in `positions_dag.py`.

---

![Bank Position Process Diagram](/position_process_diagram.png)
