cd crypto_data_project/
source env/bin/activate
cd crypto_github_data/
python github_data_pipeline_api.py > /logs/script_pipeline.log
python github_data_process.py > /logs/script_process.log
python telegram_bot_staging.py > /logs/script_bot_staging.log
