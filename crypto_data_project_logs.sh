cd crypto_data_project/
source env/bin/activate
cd crypto_github_data/
python github_data_pipeline_api.py
python github_data_process.py
python telegram_bot_staging.py
