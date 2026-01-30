@echo off
cd /d "%USERPROFILE%\Desktop\IBKR_MeanReversion_Bot"
py run_bot.py >> logs.txt 2>&1
