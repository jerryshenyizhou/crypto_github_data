cd
cd crypto_data_project/
source env/bin/activate
cd crypto_github_data/
python github_data_process.py > ./logs/script_process_$(date "+%Y.%m.%d-%H.%M.%S").log
python telegram_bot_staging.py > ./logs/script_bot_staging_$(date "+%Y.%m.%d-%H.%M.%S").log
