@echo off
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
set LOG="%~dp0git_push.log"
echo [%date% %time%] refresh start >> %LOG%
echo === WALLET SCOUT REFRESH ===
python wallet_scout_v3_2026.05.09.py --report
echo [%date% %time%] python done, errorlevel=%errorlevel% >> %LOG%
if %errorlevel% neq 0 ( echo Python FAILED & pause & goto :end )
for /f %%i in ('dir /b /od scouting_report_*.html') do set LATEST=%%i
copy /Y "%LATEST%" index.html
git add index.html wallet_cache.json sentiment_history.json war_history.json war_excluded.json data/ btc_price_cache.json smart_money_events.json cmm_api_quota.json .assetsignore
git commit -m "refresh"
echo [%date% %time%] commit done >> %LOG%
git stash
git pull --rebase origin main
echo [%date% %time%] pull done, errorlevel=%errorlevel% >> %LOG%
if %errorlevel% neq 0 ( echo PULL FAILED & git stash pop & pause & goto :end )
git stash pop
git push origin main
echo [%date% %time%] push done, errorlevel=%errorlevel% >> %LOG%
if %errorlevel% neq 0 ( echo PUSH FAILED & pause & goto :end )
echo === wrangler deploy ===
wrangler deploy
echo [%date% %time%] deploy done, errorlevel=%errorlevel% >> %LOG%
if %errorlevel% neq 0 ( echo DEPLOY FAILED ) else ( echo DEPLOY OK - homepage updated! )
:end
echo [%date% %time%] refresh end >> %LOG%
pause
