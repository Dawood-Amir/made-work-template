# Correlation Between CO2 Emission and Global Temperature

## Kaggle Authentication

To access datasets from Kaggle, authentication is required. Follow these steps to set up Kaggle authentication:

1. Visit [Kaggle account settings](https://www.kaggle.com/settings).
2. Download the `kaggle.json` file.
3. Place the `kaggle.json` file in the following directory:

   **Filepath:** `/C:\Users\<Windows-username>\.kaggle\kaggle.json`

   ```json
   {
     "username": "your_kaggle_username",
     "key": "your_kaggle_api_key"
   }
   ```

## Requirements

To ensure that you have all the necessary dependencies installed, use the provided `requirements.txt` file. Run the following command to install the required packages:

```bash
$ pip install -r requirements.txt
```

## Run Pipeline

Execute the following command to run the data engineering pipeline:

```bash
$ bash project/pipeline.sh
```

## Run Test Pipeline

Execute the following command to run the test suite for the data engineering pipeline:

```bash
$ bash project/tests.sh
```

## Report

Before checking the report, ensure that the pipeline has been run at least once.

```
 project/report.ipynb
```
**link:** [https://github.com/Dawood-Amir/made-work-template/blob/main/project/report.ipynb]

## Issues

If you encounter any issues or have suggestions for improvement, please create a new issue on GitHub. We appreciate your feedback and contributions!

**GitHub Repository:** [https://github.com/Dawood-Amir/made-work-template]

