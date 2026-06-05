pip install --upgrade twine artifacts-keyring
twine upload --repository azureartifacts dist/*
if errorlevel 1 exit /b %errorlevel%
twine upload --repository pypi dist/*